type data =
  { current_did: string option [@default None]
  ; logged_in_dids: string list [@default []] }
[@@deriving yojson {strict= false}]

let default = {current_did= None; logged_in_dids= []}

module Raw = struct
  let set_session req data =
    Dream.set_session_field req "pegasus.session"
      (data_to_yojson data |> Yojson.Safe.to_string)

  let get_session req =
    match Dream.session_field req "pegasus.session" with
    | Some data -> (
      match
        data_of_yojson (try Yojson.Safe.from_string data with _ -> `Null)
      with
      | Ok data ->
          Lwt.return_some data
      | Error _ ->
          let%lwt () = set_session req default in
          Lwt.return_some default )
    | None ->
        Lwt.return_none

  let clear_session req = Dream.set_session_field req "pegasus.session" ""

  let get_current_did req =
    match%lwt get_session req with
    | Some {current_did; _} when current_did <> None ->
        Lwt.return current_did
    | _ ->
        Lwt.return_none

  let set_current_did req did =
    match%lwt get_session req with
    | Some {logged_in_dids; _} ->
        let%lwt () = set_session req {current_did= Some did; logged_in_dids} in
        Lwt.return_unit
    | None ->
        Lwt.return_unit

  let get_logged_in_dids req =
    match%lwt get_session req with
    | Some {logged_in_dids; _} ->
        Lwt.return logged_in_dids
    | None ->
        Lwt.return []

  let set_logged_in_dids req dids =
    match%lwt get_session req with
    | Some {current_did; _} ->
        let%lwt () = set_session req {current_did; logged_in_dids= dids} in
        Lwt.return_unit
    | None ->
        Lwt.return_unit
end

open Raw

let log_in_did ?(set_current = true) req did =
  match%lwt get_session req with
  | Some {current_did; logged_in_dids} ->
      let%lwt () =
        set_session req
          { current_did= (if set_current then Some did else current_did)
          ; logged_in_dids= did :: logged_in_dids }
      in
      Lwt.return_unit
  | None ->
      Lwt.return_unit

let log_out_did req did =
  match%lwt get_session req with
  | Some {current_did; logged_in_dids} ->
      let%lwt () =
        set_session req
          { current_did
          ; logged_in_dids= List.filter (fun d -> d <> did) logged_in_dids }
      in
      Lwt.return_unit
  | None ->
      Lwt.return_unit
