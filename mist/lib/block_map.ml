module Cid_map = Map.Make (Cid)

module type S = sig
  type t

  type many = {blocks: t; missing: Cid.t list}

  val add : Lex.value -> Cid.t

  val update : Cid.t -> bytes -> unit

  val find : Cid.t -> bytes option

  val remove : Cid.t -> unit

  val find_many : Cid.t list -> many

  val mem : Cid.t -> bool

  val clear : unit -> unit

  val iter : (Cid.t -> bytes -> unit) -> unit

  val to_list : (Cid.t * bytes) list

  val to_seq : (Cid.t * bytes) Seq.t

  val cids : Cid.t list

  val add_map : t -> t

  val cardinal : unit -> int

  val byte_size : unit -> int

  val equal : t -> bool
end

module Make : S = struct
  type t = bytes Cid_map.t

  type many = {blocks: t; missing: Cid.t list}

  let map : bytes Cid_map.t ref = ref Cid_map.empty

  let add value =
    let cid, bytes = Lex.to_cbor_block value in
    map := Cid_map.add cid bytes !map ;
    cid

  let update cid bytes = map := Cid_map.add cid bytes !map

  let find cid = Cid_map.find_opt cid !map

  let remove cid =
    map := Cid_map.remove cid !map ;
    ()

  let find_many cids =
    let blocks = Cid_map.empty in
    let missing = ref [] in
    List.iter
      (fun cid ->
        match Cid_map.find_opt cid !map with
        | Some bytes ->
            ignore (Cid_map.add cid bytes blocks)
        | None ->
            missing := cid :: !missing )
      cids ;
    {blocks; missing= List.rev !missing}

  let mem cid = Cid_map.mem cid !map

  let clear () =
    Cid_map.iter (fun cid _ -> remove cid) !map ;
    ()

  let iter f = Cid_map.iter (fun cid bytes -> f cid bytes) !map

  let to_list = Cid_map.bindings !map

  let to_seq = Cid_map.to_seq !map

  let cids = Cid_map.fold (fun cid _ acc -> cid :: acc) !map []

  let add_map t =
    Cid_map.fold (fun cid bytes acc -> Cid_map.add cid bytes acc) t !map

  let cardinal () = Cid_map.cardinal !map

  let byte_size () =
    Cid_map.fold (fun _ bytes acc -> acc + Bytes.length bytes) !map 0

  let equal t = Cid_map.equal ( = ) !map t
end
