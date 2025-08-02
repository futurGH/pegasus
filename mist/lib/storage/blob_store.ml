module type S = sig
  type t

  val put_blob : t -> bytes -> Cid.t Lwt.t

  val get_bytes : t -> Cid.t -> bytes Lwt.t

  val get_seq : t -> Cid.t -> char Seq.t Lwt.t

  val has : t -> Cid.t -> bool Lwt.t

  val list : t -> Cid.t list Lwt.t

  val delete : t -> Cid.t -> unit Lwt.t

  val delete_many : t -> Cid.t list -> unit Lwt.t

  val list_refs : t -> path:string -> Cid.t list Lwt.t

  val add_ref : t -> path:string -> Cid.t -> unit Lwt.t

  val delete_ref : t -> path:string -> Cid.t -> unit Lwt.t

  val recount_refs : t -> Cid.t -> unit Lwt.t
end
