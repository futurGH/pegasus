type commit_data =
  { cid: Cid.t
  ; rev: string
  ; since: string option
  ; prev: Cid.t option
  ; relevant_blocks: Block_map.t
  ; removed_cids: Cid.Set.t }

module type Readable = sig
  type t

  val get_bytes : t -> Cid.t -> bytes option Lwt.t

  val has : t -> Cid.t -> bool Lwt.t

  val get_blocks : t -> Cid.t list -> Block_map.with_missing Lwt.t
end

module type Writable = sig
  type t

  include Readable with type t := t

  val put_block : t -> ?rev:string -> Cid.t -> bytes -> unit Lwt.t

  val put_many : t -> Block_map.t -> unit Lwt.t

  val update_root : t -> ?rev:string -> Cid.t -> unit Lwt.t

  val apply_commit : t -> commit_data -> unit Lwt.t
end
