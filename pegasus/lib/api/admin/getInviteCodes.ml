type invite_code_use =
  {used_by: string [@key "usedBy"]; used_at: string [@key "usedAt"]}
[@@deriving yojson {strict= false}]

type invite_code =
  { code: string
  ; available: int
  ; disabled: bool
  ; for_account: string [@key "forAccount"]
  ; created_by: string [@key "createdBy"]
  ; created_at: string [@key "createdAt"]
  ; uses: invite_code_use list }
[@@deriving yojson {strict= false}]

type response = {codes: invite_code list; cursor: string option [@default None]}
[@@deriving yojson {strict= false}]

let handler =
  Xrpc.handler ~auth:Admin (fun {req; db; _} ->
      let _sort = Dream.query req "sort" |> Option.value ~default:"recent" in
      let limit =
        Dream.query req "limit" |> Option.map int_of_string
        |> Option.value ~default:100
      in
      let _cursor = Dream.query req "cursor" in
      let%lwt db_codes = Data_store.list_invites ~limit db in
      let codes =
        List.map
          (fun (ic : Data_store.Types.invite_code) ->
            { code= ic.code
            ; available= ic.remaining
            ; disabled= ic.remaining = 0
            ; for_account= ic.did
            ; created_by= ""
            ; created_at= ""
            ; uses= [] } )
          db_codes
      in
      Dream.json @@ Yojson.Safe.to_string
      @@ response_to_yojson {codes; cursor= None} )
