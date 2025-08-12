module type Readable = sig
  type t

  val get_bytes : t -> Cid.t -> bytes option Lwt.t

  val has : t -> Cid.t -> bool Lwt.t

  val get_blocks : t -> Cid.t list -> Block_map.with_missing Lwt.t
end

module type Writable = sig
  type t

  include Readable with type t := t

  val put_block : t -> Cid.t -> bytes -> unit Lwt.t

  val put_many : t -> Block_map.t -> unit Lwt.t

  val delete_block : t -> Cid.t -> unit Lwt.t

  val delete_many : t -> Cid.t list -> unit Lwt.t
end
