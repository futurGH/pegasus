let version = "dev"

let handler =
  Xrpc.handler (fun _ ->
      Dream.json @@ Yojson.Safe.to_string
      @@ `Assoc [("version", `String version)] )
