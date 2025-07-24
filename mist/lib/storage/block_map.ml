module Cid_map = Map.Make (Cid)

type t = bytes Cid_map.t

type with_missing = {blocks: t; missing: Cid.t list}

let empty = Cid_map.empty

let add value m =
  let cid, bytes = Lex.to_cbor_block value in
  (Cid_map.add cid bytes m, cid)

let set = Cid_map.add

let get = Cid_map.find_opt

let remove = Cid_map.remove

let get_many cids m =
  let blocks, missing =
    List.fold_left
      (fun (b, mis) cid ->
        match get cid m with
        | Some bytes ->
            (Cid_map.add cid bytes b, mis)
        | None ->
            (b, mis @ [cid]) )
      (Cid_map.empty, []) cids
  in
  {blocks; missing= List.rev missing}

let has = Cid_map.mem

let iter = Cid_map.iter

let entries = Cid_map.bindings

let merge m m' =
  let m = Cid_map.fold (fun cid bytes m -> Cid_map.add cid bytes m) m m in
  Cid_map.fold (fun cid bytes m -> Cid_map.add cid bytes m) m' m

let size = Cid_map.cardinal

let byte_size m = Cid_map.fold (fun _ bytes acc -> acc + Bytes.length bytes) m 0

let equal = Cid_map.equal Bytes.equal
