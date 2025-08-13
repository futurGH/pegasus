type t = {connection: Caqti_lwt.connection}

include Mist.Storage.Writable_blockstore with type t := t

val connect : string -> t Lwt.t
