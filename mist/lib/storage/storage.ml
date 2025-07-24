module Block_map = Block_map

type commit_data =
  { cid: Cid.t
  ; rev: string
  ; since: string option
  ; prev: Cid.t option
  ; relevant_blocks: Block_map.t
  ; removed_cids: Cid.Set.t }

module type S = sig
  type t

  val get_root : t -> Cid.t option Lwt.t

  val put_block : t -> Cid.t -> bytes -> rev:string -> unit Lwt.t

  val put_many : t -> Block_map.t -> unit Lwt.t

  val update_root : t -> Cid.t -> rev:string -> unit Lwt.t

  val apply_commit : t -> commit_data -> unit Lwt.t

  val get_bytes : t -> Cid.t -> bytes option Lwt.t

  val has : t -> Cid.t -> bool Lwt.t

  val get_blocks : t -> Cid.t list -> Block_map.with_missing Lwt.t

  val read_obj_and_bytes : t -> Cid.t -> (Dag_cbor.value * bytes) option Lwt.t

  val read_obj : t -> Cid.t -> Dag_cbor.value option Lwt.t

  val read_record : t -> Cid.t -> Lex.repo_record
end
