type status =
  { limit: int
  ; duration_ms: int
  ; remaining_points: int
  ; ms_before_next: int
  ; consumed_points: int }

type consume_result = Ok of status | Exceeded of status | Skipped

exception Rate_limit_exceeded of status

type bucket = {mutable points: int; mutable window_start: float}

type t = {duration_ms: int; max_points: int; buckets: (string, bucket) Hashtbl.t}

let create ~duration_ms ~points =
  {duration_ms; max_points= points; buckets= Hashtbl.create 256}

let now_ms () = Unix.gettimeofday () *. 1000.0

let consume t ~key ~points =
  let now = now_ms () in
  let bucket =
    match Hashtbl.find_opt t.buckets key with
    | Some b ->
        if now -. b.window_start >= Float.of_int t.duration_ms then (
          b.points <- t.max_points ;
          b.window_start <- now ) ;
        b
    | None ->
        let b = {points= t.max_points; window_start= now} in
        Hashtbl.add t.buckets key b ;
        b
  in
  let ms_before_next =
    Float.to_int (bucket.window_start +. Float.of_int t.duration_ms -. now)
  in
  let ms_before_next = max 0 ms_before_next in
  if bucket.points < points then
    Exceeded
      { limit= t.max_points
      ; duration_ms= t.duration_ms
      ; remaining_points= bucket.points
      ; ms_before_next
      ; consumed_points= t.max_points - bucket.points }
  else (
    bucket.points <- bucket.points - points ;
    Ok
      { limit= t.max_points
      ; duration_ms= t.duration_ms
      ; remaining_points= bucket.points
      ; ms_before_next
      ; consumed_points= t.max_points - bucket.points } )

let reset t ~key = Hashtbl.remove t.buckets key

let get_tightest_limit results =
  let rec loop acc = function
    | [] ->
        acc
    | Skipped :: rest ->
        loop acc rest
    | (Exceeded _ as e) :: _ ->
        Some e
    | Ok s :: rest -> (
      match acc with
      | None ->
          loop (Some (Ok s)) rest
      | Some (Ok prev) when s.remaining_points < prev.remaining_points ->
          loop (Some (Ok s)) rest
      | Some _ ->
          loop acc rest )
  in
  loop None results

module Shared = struct
  let limiters : (string, t) Hashtbl.t = Hashtbl.create 16

  let register ~name ~duration_ms ~points =
    let l = create ~duration_ms ~points in
    Hashtbl.replace limiters name l

  let get name = Hashtbl.find_opt limiters name
end

module Route = struct
  let limiters : (string, t) Hashtbl.t = Hashtbl.create 64

  let get_or_create ~name ~duration_ms ~points =
    match Hashtbl.find_opt limiters name with
    | Some l ->
        l
    | None ->
        let l = create ~duration_ms ~points in
        Hashtbl.add limiters name l ;
        l
end
