let default_src = Logs.Src.create "pegasus"

let debug ?(src = default_src) = Logs.debug ~src

let err ?(src = default_src) = Logs.err ~src

let warn ?(src = default_src) = Logs.warn ~src

let info ?(src = default_src) = Logs.info ~src

let log_exn exn = err (fun log -> log "%s" (Printexc.to_string exn))
