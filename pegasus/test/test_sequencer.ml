open Pegasus
module D = Dag_cbor
module Dec = Dag_cbor.Decoder

let decode_frame frame =
  let h, rem = Dec.decode_first frame in
  let header_json = D.to_yojson h in
  let t =
    match Sequencer.Types.header_of_yojson header_json with
    | Ok (Message t) ->
        t
    | Ok Error ->
        failwith "expected message frame header; got error"
    | Error err ->
        failwith err
  in
  let payload = Dec.decode_to_yojson rem in
  let open Yojson.Safe.Util in
  let seq = payload |> member "seq" |> to_int in
  let time = payload |> member "time" |> to_string in
  let type_ = payload |> member "$type" |> to_string in
  (seq, t, type_, time, payload)

let with_db (f : Data_store.t -> unit Lwt.t) : unit Lwt.t =
  let tmp = Filename.temp_file "pegasus_sequencer_test" ".db" in
  let%lwt pool =
    Util.connect_sqlite ~create:true ~write:true
      (Uri.of_string ("sqlite3://" ^ tmp))
  in
  let%lwt () = Migrations.run_migrations Data_store pool in
  let%lwt () = f pool in
  Lwt.return ()

let mk_cid () =
  let block =
    Dag_cbor.encode_yojson (`Assoc [("foo", `Int (Random.int 99999))])
  in
  Cid.create Dcbor block

let mk_blocks () = Bytes.create 16

let test_bus_publish_order () =
  with_db (fun conn ->
      (* subscribe before publishing *)
      let%lwt sub = Sequencer.Bus.subscribe () in
      let did = "did:example:alice" in
      let rev = "abcdef" in
      let blocks = mk_blocks () in
      let%lwt _seq = Sequencer.sequence_identity conn ~did () in
      (* publish another event *)
      let cid = mk_cid () in
      let ops =
        [ { Sequencer.Types.action= `Create
          ; path= "app.bsky.feed.post/a1b2c3"
          ; cid= Some (mk_cid ())
          ; prev= None } ]
      in
      let%lwt _ =
        Sequencer.sequence_commit conn ~did ~commit:cid ~rev ~blocks ~ops ()
      in
      (* drain two events from bus *)
      let%lwt i1 = Sequencer.Bus.wait_next sub in
      let%lwt i2 = Sequencer.Bus.wait_next sub in
      let s1, t1, ty1, _, _ = decode_frame i1.bytes in
      let s2, t2, ty2, _, _ = decode_frame i2.bytes in
      (* strictly increasing seq and correct types *)
      Alcotest.(check bool) "seq increases" true (s2 > s1) ;
      Alcotest.(check string)
        "header t #identity" "#identity"
        (Sequencer.Types.header_t_to_string t1) ;
      Alcotest.(check string) "payload $type identity" "#identity" ty1 ;
      Alcotest.(check string)
        "header t #commit" "#commit"
        (Sequencer.Types.header_t_to_string t2) ;
      Alcotest.(check string) "payload $type commit" "#commit" ty2 ;
      Sequencer.Bus.unsubscribe sub )

let test_backfill_then_live () =
  with_db (fun conn ->
      let did = "did:example:bob" in
      (* add 3 identity events to db without publishing to bus *)
      let time0 = Util.now_ms () in
      let mk_raw did =
        let evt : Sequencer.Types.identity_evt = {did; handle= None} in
        Dag_cbor.encode_yojson @@ Sequencer.Encode.format_identity evt
      in
      let%lwt _ =
        Sequencer.DB.append_event conn ~t:`Identity ~time:(time0 + 1)
          ~data:(mk_raw (did ^ ":1"))
      in
      let%lwt _ =
        Sequencer.DB.append_event conn ~t:`Identity ~time:(time0 + 2)
          ~data:(mk_raw (did ^ ":2"))
      in
      let%lwt _ =
        Sequencer.DB.append_event conn ~t:`Identity ~time:(time0 + 3)
          ~data:(mk_raw (did ^ ":3"))
      in
      (* start live stream with cursor=0, should receive 4 events; 3 backfill + 1 live *)
      let collected = ref [] in
      let waiter, wakener = Lwt.wait () in
      let send bytes =
        let seq, t, ty, _time, _payload = decode_frame bytes in
        collected := !collected @ [(seq, t, ty)] ;
        if List.length !collected >= 4 then Lwt.wakeup_later wakener () ;
        Lwt.return_unit
      in
      let stream =
        Lwt.catch
          (fun () -> Sequencer.Live.stream_with_backfill ~conn ~cursor:(Some 0) ~send)
          (fun _ -> Lwt.return_unit)
      in
      let _ = Lwt.async (fun () -> stream) in
      (* after starting, publish one live event through bus *)
      let%lwt _ = Lwt_unix.sleep 0.05 in
      let%lwt _ = Sequencer.sequence_identity conn ~did:(did ^ ":live") () in
      (* wait until we have 4 frames, then stop the stream by raising an exception in caller *)
      let%lwt () = waiter in
      let seqs = List.map (fun (s, _, _) -> s) !collected in
      Alcotest.(check int) "received 4 frames" 4 (List.length seqs) ;
      let strictly_increasing =
        List.for_all2 (fun a b -> b > a) seqs (List.tl seqs @ [max_int])
      in
      Alcotest.(check bool) "seq strictly increasing" true strictly_increasing ;
      let tys = List.map (fun (_, t, ty) -> (t, ty)) !collected in
      List.iter
        (fun (t, ty) ->
          Alcotest.(check string)
            "header t identity" "#identity"
            (Sequencer.Types.header_t_to_string t) ;
          Alcotest.(check string) "payload $type identity" "#identity" ty )
        tys ;
      Lwt.return_unit )

let test_gap_healing () =
  with_db (fun conn ->
      let did = "did:example:carol" in
      let time0 = Util.now_ms () in
      (* add 2 identity events to db without publishing *)
      let mk_raw did =
        let evt : Sequencer.Types.identity_evt = {did; handle= None} in
        Dag_cbor.encode_yojson @@ Sequencer.Encode.format_identity evt
      in
      let%lwt _ =
        Sequencer.DB.append_event conn ~t:`Identity ~time:(time0 + 1)
          ~data:(mk_raw (did ^ ":1"))
      in
      let%lwt _ =
        Sequencer.DB.append_event conn ~t:`Identity ~time:(time0 + 2)
          ~data:(mk_raw (did ^ ":2"))
      in
      (* start streaming with cursor=0; expect to get 1,2 via backfill *)
      let collected = ref [] in
      let waiter, wakener = Lwt.wait () in
      let send bytes =
        let seq, _t, _ty, _time, _payload = decode_frame bytes in
        collected := !collected @ [seq] ;
        if List.length !collected >= 4 then Lwt.wakeup_later wakener () ;
        Lwt.return_unit
      in
      let _ =
        Lwt.async (fun () ->
            Lwt.catch
              (fun () ->
                Sequencer.Live.stream_with_backfill ~conn ~cursor:(Some 0) ~send )
              (fun _ -> Lwt.return_unit) )
      in
      (* after stream starts, directly insert identity row with seq=3, then publish identity for seq=4 via bus *)
      let%lwt _ = Lwt_unix.sleep 0.05 in
      let%lwt _ =
        Sequencer.DB.append_event conn ~t:`Identity ~time:(time0 + 3)
          ~data:(mk_raw (did ^ ":3"))
      in
      let%lwt _ = Sequencer.sequence_identity conn ~did:(did ^ ":4") () in
      let%lwt () = waiter in
      (* expect 1,2 from backfill, then healer inserts 3, then bus provides 4 *)
      Alcotest.(check (list int)) "gap healed 1..4" [1; 2; 3; 4] !collected ;
      Lwt.return_unit )

let () =
  let run_test test () =
    try Lwt_main.run (test ()) with e -> Alcotest.fail (Printexc.to_string e)
  in
  Alcotest.run "sequencer"
    [ ("bus", [("publish and order", `Quick, run_test test_bus_publish_order)])
    ; ( "live"
      , [ ("backfill then live", `Quick, run_test test_backfill_then_live)
        ; ("gap healing", `Quick, run_test test_gap_healing) ] ) ]
