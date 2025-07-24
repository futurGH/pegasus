module type S = sig
  type t

  val put_temp : t -> char Seq.t -> string Lwt.t

  val make_permanent : t -> string -> Cid.t -> unit Lwt.t

  val put_permanent : t -> Cid.t -> char Seq.t -> unit Lwt.t

  val quarantine : t -> Cid.t -> unit Lwt.t

  val unquarantine : t -> Cid.t -> unit Lwt.t

  val get_bytes : t -> Cid.t -> bytes Lwt.t

  val get_seq : t -> Cid.t -> char Seq.t Lwt.t

  val has_temp : t -> string -> bool Lwt.t

  val has_stored : t -> Cid.t -> bool Lwt.t

  val delete : t -> Cid.t -> unit Lwt.t

  val delete_many : t -> Cid.t list -> unit Lwt.t
end
