module Aside.Program

open System
open System.IO
open System.Threading
open Hopac
open Suave
open Suave.Operators
open Suave.Filters
open Suave.Successful
open Suave.ServerErrors
open Suave.RequestErrors
open Suave.Logging
open Suave.Logging.Message

let private logger = Log.create "Aside"

let env k =
  match Environment.GetEnvironmentVariable k with
  | null -> failwithf "Couldn't find key '%s' in environment" k
  | v -> v

module Auth =
  let useToken (cb: string -> WebPart): WebPart =
    fun ctx ->
      async {
        return! cb "token goes here" ctx
      }

type Processor =
  private {
    shutdownCh: Ch<IVar<unit>>
    opsCh: Ch<FileInfo * Ch<unit> * Promise<unit>>
    outputsCh: Ch<FileInfo>
  }

module Processor =
  open Hopac.Infixes

  /// Acks on taking the request and started processing it.
  let handle (file: FileInfo) (p: Processor): Alt<unit> =
    p.opsCh *<+->- fun repl nack -> file, repl, nack

  /// Shutdown the processor
  let shutdown (p: Processor): Alt<unit> =
    p.shutdownCh *<+=>- fun ack -> ack

  /// A stream of outputs that have finished processing.
  let outputs (p: Processor): Stream<FileInfo> =
    Stream.indefinitely p.outputsCh

  /// Create a new processor; a loop running on a Hopac thread, with three
  /// input channels that continously takes FileInfo inputs, acks them, processes
  /// each, and then tries to give the processed file to the outputs channel.
  ///
  let create (root: DirectoryInfo) =
    let ops, outputs, shutdown = Ch (), Ch (), Ch ()

    let rec loop () =
      Alt.choose [
        // could be batched with https://github.com/logary/RingBuffer
        ops ^=> fun (file: FileInfo, repl, nack) ->
          logger.info (
            eventX "Started processing file of size {fileSize}"
            >> setField "fileSize" file.Length)

          start (Ch.give repl () <|> nack)

          let log () =
            Job.fromAsync (
              logger.infoWithBP (
                eventX "Finished processing file {fileName}"
                >> setField "fileName" file.FullName))

          timeOutMillis 2000
          >>= log
          >>=. (Ch.give outputs file <|> timeOutMillis 500) 
          >>= loop

        shutdown ^=> fun ack ->
          Job.fromAsync (logger.infoWithBP (eventX "Processor is done."))
          >>=. IVar.fill ack ()
      ]

    Job.start (loop ()) >>-.
    { shutdownCh = shutdown; outputsCh = outputs; opsCh = ops; }

/// The configuration contains the app's bindings and CancellationTokenSource
/// for the web server.
type Config =
  { home: DirectoryInfo
    cts: CancellationTokenSource
    suave: SuaveConfig }
  static member create home =
    let cts = new CancellationTokenSource()
    let web =
      { defaultConfig with
          cancellationToken = cts.Token
          bindings = [ HttpBinding.createSimple HTTP "0.0.0.0" 8080 ]
      }
    let di = DirectoryInfo home
    if not di.Exists then invalidArg "home" (sprintf "Path '%s' did not exist." home)
    { home = di; cts = cts; suave = web }


/// This record tends to grow as your app grows.
type Runtime =
  { processor: Processor }
  static member create proc =
    { processor = proc }

/// Here's a nice Suave function for saving file uploads
module Upload =
  let post (config: Config) (runtime: Runtime) =
    Auth.useToken (fun token ctx -> async {
      try
        for file in ctx.request.files do
          let target = Path.Combine(config.home.FullName, file.fileName)
          File.Copy(file.tempFilePath, target, overwrite=true)
          do! runtime.processor |> Processor.handle (FileInfo(target)) |> Job.toAsync

        return! OK "Nom nom nom!" ctx
      with exn ->
        logger.error (eventX "Internal error" >> addExn exn)
        return! SERVICE_UNAVAILABLE "Too much right now *sigh*..." ctx
    })

/// This alternative yields (can be committed to) when the SIGINT signal
/// has been sent to the program.
let sigint =
  let signal = IVar ()
  Console.CancelKeyPress.Add(fun _ ->
    logger.info (eventX "CTRL+C or SIGINT received.")
    IVar.fill signal () |> start)
  signal :> Promise<_>

/// This is the main web server API.
let app config runtime =
  choose [
    POST >=> path "/upload" >=> Upload.post config runtime
    NOT_FOUND "Resource not found"
  ]

open Hopac.Infixes

let printFinished (p: Processor) =
  Processor.outputs p
  |> Stream.takeUntil sigint
  |> Stream.iterJob (fun output ->
    logger.infoWithBP (
      eventX "Observed finished file at {filePath}"
      >> setField "filePath" output.FullName)
    |> Job.fromAsync
  )
  |> start

/// Starts a new Suave server and processor with the given config.
/// The Alt never completes, but NACKing the Alt will shutdown the
/// server and Processor.
let server (sigint: Promise<unit>) config =
  Processor.create config.home >>= fun processor ->
  do printFinished processor
  let runtime = Runtime.create processor

  // suave
  let webapp = app config runtime
  let _, webIsRunning = startWebServerAsync config.suave webapp

  // signalling
  let closed = IVar ()
  let webserver = Job.fromAsync webIsRunning >>=. IVar.fill closed 0

  // callback function to shut things down, returning an Alt
  // that shuts down the processor
  let shutdown (): Alt<unit> =
    Alt.prepareJob <| fun () ->
    config.cts.Cancel() // cancels the web server (interop code)
    Job.fromAsync (logger.infoWithBP (eventX "Shutting down processor"))
    >>-. Processor.shutdown processor

  Job.start webserver // start Suave on another thread
  >>=. (sigint ^=> shutdown) // start awaiting the nack on another thread
  >>=. closed // wait for the server to shutdown

[<EntryPoint>]
let main argv =
  logger.info (eventX "Starting...")
  let home = Path.Combine(env "HOME", "uploads")
  let config = Config.create home
  run (server sigint config)