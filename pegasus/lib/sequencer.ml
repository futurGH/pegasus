open Lwt.Infix

module Types = struct
  let commit_op_action_to_string = function
    | `Create ->
        "create"
    | `Update ->
        "update"
    | `Delete ->
        "delete"

  let commit_op_action_of_string = function
    | "create" ->
        Ok `Create
    | "update" ->
        Ok `Update
    | "delete" ->
        Ok `Delete
    | _ ->
        Error "invalid repo op action"

  type commit_op_action =
    ([`Create | `Update | `Delete]
    [@to_yojson fun a -> `String (commit_op_action_to_string a)]
    [@of_yojson
      function
      | `String s ->
          commit_op_action_of_string s
      | _ ->
          Error "invalid repo op action"] )
  [@@deriving yojson]

  type commit_evt_op =
    { action: commit_op_action
    ; path: string
    ; cid: Cid.t option [@default None]
    ; prev: Cid.t option [@default None] }
  [@@deriving yojson]

  type commit_evt =
    { rebase: bool (* always false *)
    ; too_big: bool [@key "tooBig"] (* always false *)
    ; repo: string
    ; commit: Cid.t
    ; rev: string
    ; since: string option [@default None]
    ; blocks: bytes
    ; ops: commit_evt_op list
    ; blobs: Cid.t list (* always empty *)
    ; prev_data: Cid.t option [@key "prevData"] [@default None] }
  [@@deriving yojson]

  type sync_evt = {did: string; blocks: bytes; rev: string} [@@deriving yojson]

  type identity_evt = {did: string; handle: string option [@default None]}
  [@@deriving yojson]

  let account_status_to_string = function
    | `Active ->
        "active"
    | `Takendown ->
        "takendown"
    | `Suspended ->
        "suspended"
    | `Deleted ->
        "deleted"
    | `Deactivated ->
        "deactivated"
    | `Desynchronized ->
        "desynchronized"
    | `Throttled ->
        "throttled"

  let account_status_of_string = function
    | "active" ->
        Ok `Active
    | "takendown" ->
        Ok `Takendown
    | "suspended" ->
        Ok `Suspended
    | "deleted" ->
        Ok `Deleted
    | "deactivated" ->
        Ok `Deactivated
    | "desynchronized" ->
        Ok `Desynchronized
    | "throttled" ->
        Ok `Throttled
    | _ ->
        Error "invalid account status"

  type account_status =
    ([ `Active
     | `Takendown
     | `Suspended
     | `Deleted
     | `Deactivated
     | `Desynchronized
     | `Throttled ]
    [@to_yojson fun s -> `String (account_status_to_string s)]
    [@of_yojson
      function
      | `String s ->
          account_status_of_string s
      | _ ->
          Error "invalid account status"] )
  [@@deriving yojson]

  type account_evt =
    { did: string
    ; active: bool
    ; status: account_status option [@default Some `Active] }
  [@@deriving yojson]

  type info_evt = {name: string; message: string} [@@deriving yojson]

  type message_payload =
    | Commit of commit_evt
    | Sync of sync_evt
    | Identity of identity_evt
    | Account of account_evt
    | Info of info_evt

  type error_payload = {error: string; message: string option}
  [@@deriving yojson]

  type payload = Message of message_payload | Error of error_payload

  let header_op_to_int = function `Message -> 1 | `Error -> -1

  let header_op_of_int = function
    | 1 ->
        Ok `Message
    | -1 ->
        Ok `Error
    | _ ->
        Error "invalid header op"

  type header_op =
    ([`Message | `Error]
    [@to_yojson fun o -> `Int (header_op_to_int o)]
    [@of_yojson
      function `Int i -> header_op_of_int i | _ -> Error "invalid header op"] )
  [@@deriving yojson]

  let header_t_to_string = function
    | `Commit ->
        "#commit"
    | `Sync ->
        "#sync"
    | `Identity ->
        "#identity"
    | `Account ->
        "#account"
    | `Info ->
        "#info"

  let header_t_of_string = function
    | "#commit" ->
        Ok `Commit
    | "#sync" ->
        Ok `Sync
    | "#identity" ->
        Ok `Identity
    | "#account" ->
        Ok `Account
    | "#info" ->
        Ok `Info
    | _ ->
        Error "invalid header type"

  type header_t =
    ([`Commit | `Sync | `Identity | `Account | `Info]
    [@to_yojson fun t -> `String (header_t_to_string t)]
    [@of_yojson
      function
      | `String s ->
          header_t_of_string s
      | _ ->
          Error "invalid header type"] )
  [@@deriving yojson]

  type header = Message of header_t | Error

  let header_to_yojson = function
    | Message t ->
        `Assoc [("op", `Int 1); ("t", header_t_to_yojson t)]
    | Error ->
        `Assoc [("op", `Int (-1))]

  let header_of_yojson = function
    | `Assoc [("op", `Int -1)] ->
        Ok Error
    | `Assoc a -> (
      match (List.assoc_opt "op" a, List.assoc_opt "t" a) with
      | Some (`Int 1), Some (`String t) -> (
        match header_t_of_string t with
        | Ok t ->
            Ok (Message t)
        | Error _ ->
            Error "invalid header type" )
      | _ ->
          Error "invalid header type" )
    | _ ->
        Error "invalid header type"

  type event_kind =
    | Message of message_payload * header_t
    | Error of error_payload

  type event = {seq: int; time: string; kind: event_kind}
end

open Types

module Encode = struct
  let format_commit
      ({repo; commit; rev; since; blocks; ops; prev_data; _} : commit_evt) =
    `Assoc
      ( [ ("rebase", `Bool false)
        ; ("tooBig", `Bool false)
        ; ("repo", `String repo)
        ; ("commit", Cid.to_yojson commit)
        ; ("rev", `String rev)
        ; ("since", match since with Some s -> `String s | None -> `Null)
        ; ("blocks", Dag_cbor.to_yojson (`Bytes blocks))
        ; ("ops", `List (List.map commit_evt_op_to_yojson ops))
        ; ("blobs", `List []) ]
      @
      match prev_data with
      | Some cid ->
          [("prevData", Cid.to_yojson cid)]
      | None ->
          [] )

  let format_sync ({did; blocks; rev; _} : sync_evt) =
    `Assoc
      [ ("did", `String did)
      ; ("blocks", Dag_cbor.to_yojson (`Bytes blocks))
      ; ("rev", `String rev) ]

  let format_identity ({did; handle; _} : identity_evt) =
    let fields =
      [("did", `String did)]
      @ match handle with Some h -> [("handle", `String h)] | None -> []
    in
    `Assoc fields

  let format_account ({did; active; status} : account_evt) =
    let fields =
      [("did", `String did); ("active", `Bool active)]
      @
      match status with
      | Some s ->
          [("status", `String (account_status_to_string s))]
      | None ->
          []
    in
    `Assoc fields
end

module Frame = struct
  let message_header t =
    let header =
      `Assoc [("op", `Int 1); ("t", `String (header_t_to_string t))]
    in
    Dag_cbor.encode_yojson header

  let error_header = Dag_cbor.encode_yojson (`Assoc [("op", `Int (-1))])

  let encode_message ~seq ~time evt : bytes =
    let header, t, payload =
      match evt with
      | Commit commit ->
          ( message_header `Commit
          , header_t_to_string `Commit
          , Encode.format_commit commit )
      | Sync sync ->
          ( message_header `Sync
          , header_t_to_string `Sync
          , Encode.format_sync sync )
      | Identity identity ->
          ( message_header `Identity
          , header_t_to_string `Identity
          , Encode.format_identity identity )
      | Account account ->
          ( message_header `Account
          , header_t_to_string `Account
          , Encode.format_account account )
      | Info info ->
          ( message_header `Info
          , header_t_to_string `Info
          , info_evt_to_yojson info )
    in
    let payload' =
      match payload with
      | `Assoc a ->
          `Assoc
            ( [("$type", `String t); ("seq", `Int seq); ("time", `String time)]
            @ a )
          |> Dag_cbor.encode_yojson
      | _ ->
          failwith "evt encoded to non-object json"
    in
    Bytes.cat header payload'

  let encode_error err : bytes =
    let payload = error_payload_to_yojson err |> Dag_cbor.encode_yojson in
    Bytes.cat error_header payload
end

module Parse = struct
  let parse_commit (bytes : bytes) : (commit_evt, string) result =
    try
      let j = Dag_cbor.decode_to_yojson bytes in
      let open Yojson.Safe.Util in
      let repo = j |> member "repo" |> to_string in
      let commit = j |> member "commit" |> Cid.of_yojson |> Result.get_ok in
      let rev = j |> member "rev" |> to_string in
      let since = j |> member "since" |> to_string_option in
      let blocks =
        match j |> member "blocks" |> Dag_cbor.of_yojson with
        | `Bytes b ->
            b
        | _ ->
            failwith "invalid blocks"
      in
      let rebase = j |> member "rebase" |> to_bool in
      let too_big = j |> member "tooBig" |> to_bool in
      let blobs =
        j |> member "blobs" |> to_list
        |> List.filter_map (fun x ->
            match Cid.of_yojson x with Ok c -> Some c | _ -> None )
      in
      let prev_data =
        match j |> member "prevData" with
        | `Null ->
            None
        | v -> (
          match Cid.of_yojson v with Ok c -> Some c | _ -> None )
      in
      let ops =
        j |> member "ops" |> to_list
        |> List.map (fun opj ->
            let action =
              match opj |> member "action" |> to_string with
              | "create" ->
                  `Create
              | "update" ->
                  `Update
              | "delete" ->
                  `Delete
              | _ ->
                  `Create
            in
            let path = opj |> member "path" |> to_string in
            let cid =
              match opj |> member "cid" with
              | `Null ->
                  None
              | v -> (
                match Cid.of_yojson v with Ok c -> Some c | _ -> None )
            in
            let prev =
              match opj |> member "prev" with
              | `Null ->
                  None
              | v -> (
                match Cid.of_yojson v with Ok c -> Some c | _ -> None )
            in
            {action; path; cid; prev} )
      in
      Ok
        { rebase
        ; too_big
        ; repo
        ; commit
        ; rev
        ; since
        ; blocks
        ; ops
        ; blobs
        ; prev_data }
    with e -> Error (Printexc.to_string e)

  let parse_sync (bytes : bytes) : (sync_evt, string) result =
    try
      let j = Dag_cbor.decode_to_yojson bytes in
      let open Yojson.Safe.Util in
      let did = j |> member "did" |> to_string in
      let rev = j |> member "rev" |> to_string in
      let blocks =
        match j |> member "blocks" |> Dag_cbor.of_yojson with
        | `Bytes b ->
            b
        | _ ->
            failwith "invalid blocks"
      in
      Ok {did; blocks; rev}
    with e -> Error (Printexc.to_string e)

  let parse_identity (bytes : bytes) : (identity_evt, string) result =
    try
      let j = Dag_cbor.decode_to_yojson bytes in
      let open Yojson.Safe.Util in
      let did = j |> member "did" |> to_string in
      let handle = j |> member "handle" |> to_string_option in
      Ok {did; handle}
    with e -> Error (Printexc.to_string e)

  let parse_account (bytes : bytes) : (account_evt, string) result =
    try
      let j = Dag_cbor.decode_to_yojson bytes in
      let open Yojson.Safe.Util in
      let did = j |> member "did" |> to_string in
      let active = j |> member "active" |> to_bool in
      let status =
        j |> member "status" |> to_string_option
        |> Option.map (fun s -> Result.get_ok @@ account_status_of_string s)
      in
      Ok {did; active; status}
    with e -> Error (Printexc.to_string e)

  let event_of_db_event (dbe : Data_store.Types.firehose_event) =
    let t = header_t_of_string dbe.t in
    match t with
    | Ok t -> (
        let kind_result =
          match t with
          | `Commit ->
              parse_commit dbe.data
              |> Result.map (fun evt -> Message (Commit evt, t))
          | `Sync ->
              parse_sync dbe.data
              |> Result.map (fun evt -> Message (Sync evt, t))
          | `Identity ->
              parse_identity dbe.data
              |> Result.map (fun evt -> Message (Identity evt, t))
          | `Account ->
              parse_account dbe.data
              |> Result.map (fun evt -> Message (Account evt, t))
          | `Info ->
              Error "Info events not supported in DB"
        in
        match kind_result with
        | Ok kind ->
            Ok {seq= dbe.seq; time= Util.ms_to_iso8601 dbe.time; kind}
        | Error e ->
            Error ("failed to parse event: " ^ e) )
    | Error _ ->
        Error ("invalid header type " ^ dbe.t)
end

module Bus = struct
  type item = {seq: int; bytes: bytes}

  let ring_size = 2048

  let queue_max = 1000

  let ring : item array = Array.make ring_size {seq= 0; bytes= Bytes.empty}

  let head_seq = ref 0

  let count = ref 0

  type subscriber =
    { id: int
    ; q: item Queue.t
    ; cond: unit Lwt_condition.t
    ; mutable closed: bool
    ; mutable close_reason: string option }

  let subs : (int, subscriber) Hashtbl.t = Hashtbl.create 64

  let next_id = ref 1

  let lock = Lwt_mutex.create ()

  let publish (it : item) =
    Lwt_mutex.with_lock lock (fun () ->
        head_seq := it.seq ;
        ring.(it.seq mod ring_size) <- it ;
        if !count < ring_size then incr count ;
        Hashtbl.iter
          (fun _ s ->
            if not s.closed then (
              Queue.push it s.q ;
              if Queue.length s.q > queue_max then (
                s.closed <- true ;
                s.close_reason <- Some "ConsumerTooSlow" ;
                Hashtbl.remove subs s.id ;
                Lwt_condition.broadcast s.cond () )
              else Lwt_condition.broadcast s.cond () ) )
          subs ;
        Lwt.return_unit )

  let latest_seq () = !head_seq

  let subscribe () =
    Lwt_mutex.with_lock lock (fun () ->
        let id = next_id in
        incr next_id ;
        let s =
          { id= !id
          ; q= Queue.create ()
          ; cond= Lwt_condition.create ()
          ; closed= false
          ; close_reason= None }
        in
        Hashtbl.add subs !id s ; Lwt.return s )

  let unsubscribe (s : subscriber) =
    Lwt_mutex.with_lock lock (fun () ->
        s.closed <- true ;
        Hashtbl.remove subs s.id ;
        Lwt.return_unit )

  let ring_after (after : int) : item list =
    if !head_seq <= after then []
    else
      let first = max (!head_seq - !count + 1) (after + 1) in
      if first > !head_seq then []
      else
        let rec collect acc seq =
          if seq > !head_seq then List.rev acc
          else
            let it = ring.(seq mod ring_size) in
            collect (it :: acc) (seq + 1)
        in
        collect [] first

  let rec wait_next (s : subscriber) : item Lwt.t =
    if s.closed then failwith "subscriber closed"
    else if not (Queue.is_empty s.q) then Lwt.return (Queue.pop s.q)
    else Lwt_condition.wait s.cond >>= fun () -> wait_next s
end

module DB = struct
  let append_event (conn : Data_store.t) ~(time : int) ~(t : header_t)
      ~(data : bytes) : int Lwt.t =
    Data_store.append_firehose_event conn ~time ~t:(header_t_to_string t) ~data

  let request_seq_range ?earliest_seq ?latest_seq ?earliest_time ?(limit = 100)
      (conn : Data_store.t) : event list Lwt.t =
    let%lwt rows =
      match (earliest_time, earliest_seq) with
      | Some _t, _ -> (
          Data_store.earliest_firehose_after_time conn
            ~time:(Option.get earliest_time)
          >>= function
          | None ->
              Lwt.return []
          | Some first ->
              Data_store.list_firehose_since conn ~since:(first.seq - 1) ~limit
          )
      | None, Some since ->
          Data_store.list_firehose_since conn ~since ~limit
      | None, None ->
          Data_store.list_firehose_since conn ~since:0 ~limit
    in
    let evs =
      List.filter_map
        (fun (r : Data_store.Types.firehose_event) ->
          match latest_seq with
          | Some max when r.seq > max ->
              None
          | _ -> (
            match Parse.event_of_db_event r with
            | Ok e ->
                Some e
            | Error _ ->
                None ) )
        rows
    in
    Lwt.return evs

  let latest_seq (conn : Data_store.t) : int Lwt.t =
    Data_store.latest_firehose_seq conn >|= function Some s -> s | None -> 0
end

module Live = struct
  let stream_with_backfill ~(conn : Data_store.t) ~(cursor : int)
      ~(send : bytes -> unit Lwt.t) : unit Lwt.t =
    let%lwt sub = Bus.subscribe () in
    let send_consumer_too_slow () =
      let err =
        { error= "ConsumerTooSlow"
        ; message=
            Some
              "you're not consuming messages fast enough! maybe \
               com.atproto.sync.getRepo is more your speed?" }
      in
      send (Frame.encode_error err)
    in
    Lwt.finalize
      (fun () ->
        let%lwt head_db = DB.latest_seq conn in
        let cutoff = head_db in
        (* try backfill from buffer first *)
        let ring = Bus.ring_after cursor in
        let ring_covers =
          match List.rev ring with
          | [] ->
              false
          | last :: _ ->
              last.seq >= cutoff
        in
        ( if ring_covers then
            Lwt_list.iter_s (fun (it : Bus.item) -> send it.bytes) ring
          else
            let%lwt events =
              DB.request_seq_range ~earliest_seq:cursor ~latest_seq:cutoff
                ~limit:1000 conn
            in
            Lwt_list.iter_s
              (fun ev ->
                match ev.kind with
                | Error _ ->
                    Lwt.return_unit
                | Message (payload, _) ->
                    send
                      (Frame.encode_message ~seq:ev.seq ~time:ev.time payload) )
              events )
        >>= fun () ->
        (* bail if consumer too slow *)
        if sub.Bus.closed then
          match sub.Bus.close_reason with
          | Some "ConsumerTooSlow" ->
              send_consumer_too_slow ()
          | _ ->
              Lwt.return_unit
        else
          (* live tail *)
          let rec loop last =
            if sub.Bus.closed then
              match sub.Bus.close_reason with
              | Some "ConsumerTooSlow" ->
                  send_consumer_too_slow ()
              | _ ->
                  Lwt.return_unit
            else
              Lwt.catch
                (fun () ->
                  let%lwt it = Bus.wait_next sub in
                  if it.seq <= last then loop last
                  else if it.seq > last + 1 then
                    let%lwt gap =
                      DB.request_seq_range ~earliest_seq:last ~latest_seq:it.seq
                        ~limit:1000 conn
                    in
                    let%lwt () =
                      Lwt_list.iter_s
                        (fun ev ->
                          if ev.seq <= last then Lwt.return_unit
                          else
                            send
                              ( match ev.kind with
                              | Message (m, _) ->
                                  Frame.encode_message ~seq:ev.seq ~time:ev.time
                                    m
                              | Error e ->
                                  Frame.encode_error e ) )
                        gap
                    in
                    send it.bytes >>= fun () -> loop it.seq
                  else send it.bytes >>= fun () -> loop it.seq )
                (fun _exn ->
                  (* check if any failure was due to slow consumer *)
                  match sub.Bus.close_reason with
                  | Some "ConsumerTooSlow" ->
                      send_consumer_too_slow ()
                  | _ ->
                      Lwt.return_unit )
          in
          loop cutoff )
      (fun () -> Bus.unsubscribe sub)
end

let sequence_commit (conn : Data_store.t) ~(did : string) ~(commit : Cid.t)
    ~(rev : string) ?since ~(blocks : bytes) ~(ops : commit_evt_op list)
    ?(prev_data : Cid.t option) () : int Lwt.t =
  let time_ms = Util.now_ms () in
  let time_iso = Util.ms_to_iso8601 time_ms in
  let evt : commit_evt =
    { rebase= false
    ; too_big= false
    ; blobs= []
    ; repo= did
    ; commit
    ; rev
    ; since
    ; blocks
    ; ops
    ; prev_data }
  in
  let raw = Dag_cbor.encode_yojson @@ Encode.format_commit evt in
  let%lwt seq = DB.append_event conn ~t:`Commit ~time:time_ms ~data:raw in
  let frame = Frame.encode_message ~seq ~time:time_iso (Commit evt) in
  let%lwt () = Bus.publish {seq; bytes= frame} in
  Lwt.return seq

let sequence_sync (conn : Data_store.t) ~(did : string) ~(rev : string)
    ~(blocks : bytes) () : int Lwt.t =
  let time_ms = Util.now_ms () in
  let time_iso = Util.ms_to_iso8601 time_ms in
  let evt : sync_evt = {did; rev; blocks} in
  let raw = Dag_cbor.encode_yojson @@ Encode.format_sync evt in
  let%lwt seq = DB.append_event conn ~t:`Sync ~time:time_ms ~data:raw in
  let frame = Frame.encode_message ~seq ~time:time_iso (Sync evt) in
  let%lwt () = Bus.publish {seq; bytes= frame} in
  Lwt.return seq

let sequence_identity (conn : Data_store.t) ~(did : string)
    ?(handle : string option) () : int Lwt.t =
  let time_ms = Util.now_ms () in
  let time_iso = Util.ms_to_iso8601 time_ms in
  let evt : identity_evt = {did; handle} in
  let raw = Dag_cbor.encode_yojson @@ Encode.format_identity evt in
  let%lwt seq = DB.append_event conn ~t:`Identity ~time:time_ms ~data:raw in
  let frame = Frame.encode_message ~seq ~time:time_iso (Identity evt) in
  let%lwt () = Bus.publish {seq; bytes= frame} in
  Lwt.return seq

let sequence_account (conn : Data_store.t) ~(did : string) ~(active : bool)
    ?(status : account_status option) () : int Lwt.t =
  let time_ms = Util.now_ms () in
  let time_iso = Util.ms_to_iso8601 time_ms in
  let evt : account_evt = {did; active; status} in
  let raw = Dag_cbor.encode_yojson @@ Encode.format_account evt in
  let%lwt seq = DB.append_event conn ~t:`Account ~time:time_ms ~data:raw in
  let frame = Frame.encode_message ~seq ~time:time_iso (Account evt) in
  let%lwt () = Bus.publish {seq; bytes= frame} in
  Lwt.return seq
