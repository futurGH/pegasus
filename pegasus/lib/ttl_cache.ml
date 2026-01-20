module Make (K : Hashtbl.HashedType) = struct
  module H = Hashtbl.Make (K)

  type time_ms = int

  type 'a entry = {value: 'a; mutable expires_at: time_ms}

  type 'a t =
    {table: 'a entry H.t; mutable capacity: int option; default_ttl: time_ms}

  let default_initial_capacity = 16

  let[@inline] _now_ms () : time_ms = Util.Time.now_ms ()

  let create ?capacity ?(initial_capacity = default_initial_capacity)
      default_ttl () : 'a t =
    {table= H.create initial_capacity; capacity; default_ttl}

  let clear (t : 'a t) : unit = H.clear t.table

  let remove (t : 'a t) (k : K.t) : unit = H.remove t.table k

  let[@inline] _is_expired ~now (e : _ entry) = e.expires_at <= now

  let cleanup (t : 'a t) : unit =
    let now = _now_ms () in
    (* collect first to avoid mutating while iterating *)
    let to_remove = ref [] in
    H.iter
      (fun k e -> if _is_expired ~now e then to_remove := k :: !to_remove)
      t.table ;
    List.iter (H.remove t.table) !to_remove

  let _find_entry_opt (t : 'a t) (k : K.t) : 'a entry option =
    try Some (H.find t.table k) with Not_found -> None

  let get (t : 'a t) (k : K.t) : 'a option =
    let now = _now_ms () in
    match _find_entry_opt t k with
    | None ->
        None
    | Some e ->
        if _is_expired ~now e then (
          (* lazy eviction *)
          H.remove t.table k ;
          None )
        else Some e.value

  let mem (t : 'a t) (k : K.t) : bool =
    match get t k with None -> false | Some _ -> true

  let length (t : 'a t) : int = cleanup t ; H.length t.table

  let _evict_earliest (t : 'a t) : unit =
    let earliest_key = ref None in
    let earliest_exp = ref max_int in
    H.iter
      (fun k e ->
        if e.expires_at < !earliest_exp then (
          earliest_exp := e.expires_at ;
          earliest_key := Some k ) )
      t.table ;
    match !earliest_key with None -> () | Some k -> H.remove t.table k

  let _enforce_capacity_after_insert (t : 'a t) : unit =
    match t.capacity with
    | None ->
        ()
    | Some cap ->
        cleanup t ;
        while H.length t.table > cap do
          _evict_earliest t
        done

  let set ?ttl_ms (t : 'a t) (k : K.t) (v : 'a) : unit =
    let now = _now_ms () in
    let ttl_ms = Option.value ttl_ms ~default:t.default_ttl in
    if ttl_ms <= 0 then H.remove t.table k
    else
      let expires_at = now + ttl_ms in
      let entry = {value= v; expires_at} in
      H.replace t.table k entry ;
      _enforce_capacity_after_insert t

  let replace = set

  let ttl_remaining_ms (t : 'a t) (k : K.t) : int option =
    let now = _now_ms () in
    match _find_entry_opt t k with
    | None ->
        None
    | Some e ->
        if _is_expired ~now e then (H.remove t.table k ; None)
        else Some (e.expires_at - now)

  let to_list (t : 'a t) : (K.t * 'a) list =
    cleanup t ;
    let acc = ref [] in
    H.iter (fun k e -> acc := (k, e.value) :: !acc) t.table ;
    !acc

  let iter (t : 'a t) ~(f : K.t -> 'a -> unit) : unit =
    cleanup t ;
    H.iter (fun k e -> f k e.value) t.table

  let fold (t : 'a t) ~(init : 'acc) ~(f : 'acc -> K.t -> 'a -> 'acc) : 'acc =
    cleanup t ;
    H.fold (fun k e acc -> f acc k e.value) t.table init

  let set_capacity (t : 'a t) (cap : int option) : unit =
    t.capacity <- cap ;
    _enforce_capacity_after_insert t
end

module String_cache = Make (struct
  type t = string

  let equal = String.equal

  let hash = Hashtbl.hash
end)
