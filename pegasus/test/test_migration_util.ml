open Alcotest
open Pegasus

let blob_ref cid =
  Mist.Blob_ref.of_json_ref
    (Mist.Blob_ref.Typed
       {type'= "blob"; ref= cid; mime_type= "image/png"; size= 3L} )

let test_nested_blob_refs_are_unique () =
  let first = Cid.create Raw (Bytes.of_string "one") in
  let second = Cid.create Raw (Bytes.of_string "two") in
  let first_ref = blob_ref first in
  let second_ref = blob_ref second in
  let nested =
    `LexArray
      [| `BlobRef first_ref
       ; `LexMap
           (Mist.Lex.String_map.of_list
              [("same", `BlobRef first_ref); ("other", `BlobRef second_ref)] )
      |]
  in
  let record =
    Mist.Lex.String_map.of_list
      [("direct", `BlobRef second_ref); ("nested", nested)]
  in
  let refs = Util.find_blob_refs record in
  check int "unique CIDs" 2 (List.length refs) ;
  check bool "first present" true
    (List.exists (fun (ref : Mist.Blob_ref.t) -> Cid.equal ref.ref first) refs) ;
  check bool "second present" true
    (List.exists (fun (ref : Mist.Blob_ref.t) -> Cid.equal ref.ref second) refs)

let test_repository_commit_signature () =
  let private_key, public_key_bytes = Kleidos.K256.generate_keypair () in
  let public_key : Kleidos.key =
    (public_key_bytes, (module Kleidos.K256 : Kleidos.CURVE))
  in
  let unsigned : User_store.Types.commit =
    { did= "did:plc:migrationtest"
    ; version= 3
    ; data= Cid.create Dcbor (Bytes.of_string "mst")
    ; rev= Mist.Tid.now ()
    ; prev= None }
  in
  let signature =
    User_store.Types.commit_to_yojson unsigned
    |> Dag_cbor.encode_yojson
    |> fun message -> Kleidos.K256.sign ~privkey:private_key ~msg:message
  in
  let signed : User_store.Types.signed_commit =
    { did= unsigned.did
    ; version= unsigned.version
    ; data= unsigned.data
    ; rev= unsigned.rev
    ; prev= unsigned.prev
    ; signature }
  in
  check bool "valid signature" true
    (Repository.verify_commit_signature ~public_key signed) ;
  let forged = {signed with did= "did:plc:attacker"} in
  check bool "tampered commit" false
    (Repository.verify_commit_signature ~public_key forged)

let test_private_source_addresses () =
  let is_private = Api.Account_.Migrate.is_private_source_address in
  check bool "loopback" true (is_private "127.0.0.1") ;
  check bool "rfc1918" true (is_private "192.168.1.2") ;
  check bool "ipv6 loopback" true (is_private "::1") ;
  check bool "mapped loopback" true (is_private "::ffff:127.0.0.1") ;
  check bool "public" false (is_private "1.1.1.1")

let () =
  run "migration_util"
    [ ( "blob references"
      , [("nested and duplicate", `Quick, test_nested_blob_refs_are_unique)] )
    ; ( "repository security"
      , [("signed commit", `Quick, test_repository_commit_signature)] )
    ; ( "source security"
      , [("private addresses", `Quick, test_private_source_addresses)] ) ]
