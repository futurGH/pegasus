module type Template = sig
  type props

  val props_of_json : Yojson.Basic.t -> props

  val props_to_json : props -> Yojson.Basic.t

  val make : ?key:string -> props:props -> unit -> React.element
end

let render_page ?status ?title (type props)
    (template : (module Template with type props = props)) ~props =
  let module Template = (val template : Template with type props = props) in
  let props_json = Template.props_to_json props |> Yojson.Basic.to_string in
  let page_data = Printf.sprintf "window.__PAGE__ = {props: %s};" props_json in
  let app = Template.make ~props () in
  let page =
    Frontend.Layout.make ?title ~favicon:Env.favicon_url ~children:app ()
  in
  Dream.stream ?status
    ~headers:[("Content-Type", "text/html")]
    (fun stream ->
      [%lwt
        let html, subscribe =
          ReactServerDOM.render_html ~skipRoot:false
            ~bootstrapScriptContent:page_data
            ~bootstrapScripts:["/public/client.js"] page
        in
        [%lwt
          let () = Dream.write stream html in
          [%lwt
            let () = Dream.flush stream in
            [%lwt
              let () =
                subscribe (fun chunk ->
                    [%lwt
                      let () = Dream.write stream chunk in
                      Dream.flush stream] )
              in
              Dream.flush stream]]]] )

let make_data_uri ~mimetype ~data =
  let base64_data = data |> Bytes.to_string |> Base64.encode_string in
  Printf.sprintf "data:%s;base64,%s" mimetype base64_data
