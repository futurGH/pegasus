open Mist

type commit =
  { did: string
  ; version: int (* always 3 *)
  ; data: Cid.t
  ; rev: Tid.t
  ; prev: Cid.t option }

type signed_commit =
  { did: string
  ; version: int (* always 3 *)
  ; data: Cid.t [@of_yojson Cid.of_yojson] [@to_yojson Cid.to_yojson]
  ; rev: Tid.t
  ; prev: Cid.t option
        [@of_yojson
          function
          | `Assoc link ->
              link |> List.assoc "$link" |> Cid.of_yojson
              |> Result.map (fun cid -> Some cid)
          | `Null ->
              Ok None
          | _ ->
              Error "commit prev not a valid cid"]
        [@to_yojson function Some cid -> Cid.to_yojson cid | None -> `Null]
  ; signature: bytes
        [@key "sig"]
        [@of_yojson
          fun x ->
            match Dag_cbor.of_yojson x with
            | `Bytes b ->
                Ok b
            | _ ->
                Error "commit sig not a valid bytes value"]
        [@to_yojson fun x -> Dag_cbor.to_yojson (`Bytes x)] }
[@@deriving yojson]

type signing_key = P256 of bytes | K256 of bytes

module Make (Store : Storage.Writable_blockstore) = struct
  type t = {store: Store.t; key: signing_key}
end
