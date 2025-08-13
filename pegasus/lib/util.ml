module Syntax = struct
  let ( let$! ) m f =
    match%lwt m with Ok x -> f x | Error e -> failwith (Caqti_error.show e)
end
