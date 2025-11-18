let debug = Sys.getenv_opt "DEMO_ENV" == Some "development"

let is_react_component_header str =
  String.equal str "application/react.component"

let stream_model ~location app =
  Dream.stream
    ~headers:
      [ ("Content-Type", "application/react.component")
      ; ("X-Content-Type-Options", "nosniff")
      ; ("X-Location", location) ]
    (fun stream ->
      [%lwt
        let () =
          ReactServerDOM.render_model ~debug
            ~subscribe:(fun chunk ->
              if debug then (Dream.log "Chunk" ; Dream.log "%s" chunk) ;
              [%lwt
                let () = Dream.write stream chunk in
                Dream.flush stream] )
            app
        in
        Dream.flush stream] )

let stream_html ?(skipRoot = false) ?bootstrapScriptContent
    ?(bootstrapScripts = []) ?(bootstrapModules = []) app =
  Dream.stream
    ~headers:[("Content-Type", "text/html")]
    (fun stream ->
      [%lwt
        let html, subscribe =
          ReactServerDOM.render_html ~skipRoot ?bootstrapScriptContent
            ~bootstrapScripts ~bootstrapModules ~debug app
        in
        [%lwt
          let () = Dream.write stream html in
          [%lwt
            let () = Dream.flush stream in
            [%lwt
              let () =
                subscribe (fun chunk ->
                    if debug then (Dream.log "Chunk" ; Dream.log "%s" chunk) ;
                    [%lwt
                      let () = Dream.write stream chunk in
                      Dream.flush stream] )
              in
              Dream.flush stream]]]] )

let createFromRequest ?(disableSSR = false) ?(layout = fun children -> children)
    ?(bootstrapModules = []) ?(bootstrapScripts = [])
    ?(bootstrapScriptContent = "") element request =
  match Dream.header request "accept" with
  | Some accept when is_react_component_header accept ->
      stream_model ~location:(Dream.target request) (React.Model.Element element)
  | _ ->
      stream_html ~skipRoot:disableSSR ~bootstrapScriptContent ~bootstrapScripts
        ~bootstrapModules (layout element)
