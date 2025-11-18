module DOM = Webapi.Dom
module Location = DOM.Location
module History = DOM.History
module ReadableStream = Webapi.ReadableStream

external setNavigate : DOM.Window.t -> (string -> unit) -> unit = "__navigate"
[@@mel.scope "window"] [@@mel.set]

external readable_stream : ReadableStream.t
  = "window.srr_stream.readable_stream"

let fetchApp url =
  let obj = [%mel.obj {accept= "application/react.component"}] in
  let headers = Fetch.HeadersInit.make obj in
  Fetch.fetchWithInit url
    (Fetch.RequestInit.make ~method_:Fetch.Get ~headers ())

let navigate ~setLayout search =
  let location = DOM.Window.location DOM.window in
  let origin = Location.origin location in
  let pathname = Location.pathname location in
  let currentSearch = Location.search location in
  let currentParams = URL.SearchParams.makeExn currentSearch in
  let newSearchParams = Js.Dict.empty () in
  URL.SearchParams.forEach currentParams (fun value key ->
      Js.Dict.set newSearchParams key value ) ;
  let newParams = URL.SearchParams.makeExn search in
  URL.SearchParams.forEach newParams (fun value key ->
      Js.Dict.set newSearchParams key value ) ;
  let finalSearch =
    newSearchParams |> Js.Dict.entries |> URL.SearchParams.makeWithArray
    |> URL.SearchParams.toString
  in
  if currentSearch = "?" ^ finalSearch then ()
  else
    let domain = URL.makeExn (origin ^ pathname) in
    let finalURL =
      URL.setSearch domain (URL.SearchParams.makeExn finalSearch)
    in
    let response = fetchApp (URL.toString finalURL) in
    ReactServerDOMEsbuild.createFromFetch response
    |> Js.Promise.then_ (fun element ->
        History.pushState
          (History.state DOM.history)
          "" (URL.toString finalURL) DOM.history ;
        setLayout (fun _ -> element) ;
        Js.Promise.resolve () )
    |> ignore

module App_from_readable_stream = struct
  let initial = ReactServerDOMEsbuild.createFromReadableStream readable_stream

  let[@react.component] make () =
    let initialElement = React.Experimental.usePromise initial in
    let layout, setLayout = React.useState (fun () -> initialElement) in
    setNavigate DOM.window (navigate ~setLayout) ;
    let fallback error =
      Js.log error ;
      React.createElement
        (ReactDOM.stringToComponent "h1")
        (ReactDOM.domProps ~children:(React.string "Something went wrong!") ())
    in
    React.createElement ReasonReactErrorBoundary.make
      (ReasonReactErrorBoundary.makeProps ~fallback ~children:layout ())
end

let () =
  let document = DOM.Document.asHtmlDocument DOM.document in
  let body = Option.bind document DOM.HtmlDocument.body in
  match body with
  | Some element ->
      let transition () =
        ReactDOM.Client.hydrateRoot element
          (React.createElement App_from_readable_stream.make
             (App_from_readable_stream.makeProps ()) )
        |> ignore
      in
      React.startTransition transition
  | None ->
      Js.log "Root element not found"
