type data =
  { current_did: string option [@default None]
  ; logged_in_dids: string list [@default []]
  ; session_id: string option [@default None]
  ; admin_authenticated: bool [@default false] }
[@@deriving yojson {strict= false}]

type actor = Frontend.AccountSwitcher.actor =
  {did: string; handle: string; avatar_data_uri: string option}
[@@deriving yojson {strict= false}]

let default =
  { current_did= None
  ; logged_in_dids= []
  ; session_id= None
  ; admin_authenticated= false }

type cache_entry = {timestamp: float; data: data}

let cache : (string, cache_entry) Hashtbl.t = Hashtbl.create 100

let cache_ttl = 300.0

module Raw = struct
  let set_session req data =
    let session_id = Option.value data.session_id ~default:(Mist.Tid.now ()) in
    let data_with_id = {data with session_id= Some session_id} in
    Hashtbl.replace cache session_id
      {timestamp= Unix.time (); data= data_with_id} ;
    Dream.set_session_field req "pegasus.session"
      (data_to_yojson data_with_id |> Yojson.Safe.to_string)

  let get_session req =
    match Dream.session_field req "pegasus.session" with
    | Some data_str -> (
      match
        data_of_yojson (try Yojson.Safe.from_string data_str with _ -> `Null)
      with
      | Ok data -> (
        match data.session_id with
        | Some session_id -> (
          match Hashtbl.find_opt cache session_id with
          | Some entry when Unix.time () -. entry.timestamp < cache_ttl ->
              Lwt.return_some entry.data
          | _ ->
              Hashtbl.replace cache session_id {timestamp= Unix.time (); data} ;
              Lwt.return_some data )
        | None ->
            let session_id = Mist.Tid.now () in
            let data_with_id = {data with session_id= Some session_id} in
            Hashtbl.replace cache session_id
              {timestamp= Unix.time (); data= data_with_id} ;
            let%lwt () = set_session req data_with_id in
            Lwt.return_some data_with_id )
      | Error _ ->
          let%lwt () = set_session req default in
          Lwt.return_some default )
    | None ->
        Lwt.return_none

  let clear_session req =
    let%lwt () =
      match%lwt get_session req with
      | Some {session_id= Some id; _} ->
          Hashtbl.remove cache id ; Lwt.return_unit
      | _ ->
          Lwt.return_unit
    in
    Dream.set_session_field req "pegasus.session" ""

  let set_current_did req did =
    match%lwt get_session req with
    | Some {logged_in_dids; session_id; admin_authenticated; _} ->
        let%lwt () =
          set_session req
            { current_did= Some did
            ; logged_in_dids
            ; session_id
            ; admin_authenticated }
        in
        Lwt.return_unit
    | None ->
        Lwt.return_unit

  let get_current_did req =
    match%lwt get_session req with
    | Some {current_did= Some did; _} ->
        Lwt.return_some did
    | Some {logged_in_dids= first :: _; _} ->
        let%lwt () = set_current_did req first in
        Lwt.return_some first
    | _ ->
        Lwt.return_none

  let get_logged_in_dids req =
    match%lwt get_session req with
    | Some {logged_in_dids; _} ->
        Lwt.return logged_in_dids
    | None ->
        Lwt.return []

  let set_logged_in_dids req dids =
    match%lwt get_session req with
    | Some {current_did; session_id; admin_authenticated; _} ->
        let%lwt () =
          set_session req
            {current_did; logged_in_dids= dids; session_id; admin_authenticated}
        in
        Lwt.return_unit
    | None ->
        Lwt.return_unit
end

open Raw

let log_in_did req did =
  match%lwt get_session req with
  | Some {logged_in_dids; session_id; admin_authenticated; _} ->
      let%lwt () =
        set_session req
          { current_did= Some did
          ; logged_in_dids=
              ( if List.mem did logged_in_dids then logged_in_dids
                else did :: logged_in_dids )
          ; session_id
          ; admin_authenticated }
      in
      Lwt.return_unit
  | None ->
      set_session req
        { current_did= Some did
        ; logged_in_dids= [did]
        ; session_id= None
        ; admin_authenticated= false }

let log_out_did req did =
  match%lwt get_session req with
  | Some {current_did; logged_in_dids; session_id; admin_authenticated} ->
      let logged_in_dids = List.filter (fun d -> d <> did) logged_in_dids in
      let current_did =
        if current_did = Some did then List.nth_opt logged_in_dids 0
        else current_did
      in
      let%lwt () =
        set_session req
          {current_did; logged_in_dids; session_id; admin_authenticated}
      in
      Lwt.return_unit
  | None ->
      Lwt.return_unit

let log_out_all_dids req =
  match%lwt get_session req with
  | Some session ->
      let%lwt () =
        set_session req {session with logged_in_dids= []; current_did= None}
      in
      Lwt.return_unit
  | None ->
      Lwt.return_unit

let is_logged_in req did =
  match%lwt get_session req with
  | Some {current_did; logged_in_dids; _} ->
      Lwt.return (current_did = Some did || List.mem did logged_in_dids)
  | None ->
      Lwt.return false

let list_logged_in_actors req db =
  match%lwt get_logged_in_dids req with
  | [] ->
      Lwt.return (None, [])
  | dids ->
      let%lwt current_did = Raw.get_current_did req in
      let%lwt actors =
        Lwt_list.filter_map_s
          (fun did ->
            match%lwt Data_store.get_actor_by_identifier did db with
            | Some {handle; _} -> (
                let actor = {did; handle; avatar_data_uri= None} in
                let%lwt us = User_store.connect did in
                match%lwt
                  User_store.get_record us "app.bsky.actor.profile/self"
                with
                | Some {value= profile; _} -> (
                  match Mist.Lex.String_map.find_opt "avatar" profile with
                  | Some (`BlobRef {ref; _}) -> (
                    match%lwt User_store.get_blob us ref with
                    | Some {data; mimetype; _}
                      when String.starts_with ~prefix:"image/" mimetype ->
                        Lwt.return_some
                          { actor with
                            avatar_data_uri=
                              Some (Util.make_data_uri ~mimetype ~data) }
                    | _ ->
                        Lwt.return_some actor )
                  | _ ->
                      Lwt.return_some actor )
                | None ->
                    Lwt.return_some actor )
            | _ ->
                Lwt.return_none )
          dids
      in
      let current_actor =
        List.find_opt (fun (a : actor) -> Some a.did = current_did) actors
      in
      Lwt.return (current_actor, actors)

let set_admin_authenticated req authenticated =
  match%lwt get_session req with
  | Some {current_did; logged_in_dids; session_id; _} ->
      set_session req
        { current_did
        ; logged_in_dids
        ; session_id
        ; admin_authenticated= authenticated }
  | None ->
      set_session req
        { current_did= None
        ; logged_in_dids= []
        ; session_id= None
        ; admin_authenticated= authenticated }

let is_admin_authenticated req =
  match%lwt get_session req with
  | Some {admin_authenticated; _} ->
      Lwt.return admin_authenticated
  | None ->
      Lwt.return false
