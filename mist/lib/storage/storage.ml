module Block_map = Block_map
module Blob_store = Blob_store

module type Readable_blockstore = Repo_store.Readable

module type Writable_blockstore = Repo_store.Writable

module Memory_blockstore = struct
  module Impl = Memory_store.Make ()
  include Impl

  module Readable : Repo_store.Readable with type t = Impl.t = Impl

  module Writable : Repo_store.Writable with type t = Impl.t = Impl
end

module Overlay_blockstore
    (Top : Repo_store.Readable)
    (Bottom : Repo_store.Readable) =
struct
  module Impl = Overlay_store.Make (Top) (Bottom)
  include Impl

  module Readable : Repo_store.Readable with type t = Impl.t = Impl
end

type commit_data = Repo_store.commit_data
