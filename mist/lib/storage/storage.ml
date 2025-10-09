module Block_map = Block_map
module Blob_store = Blob_store

module type Readable_blockstore = Blockstore.Readable

module type Writable_blockstore = Blockstore.Writable

module Memory_blockstore = struct
  module Impl = Memory_blockstore.Make ()
  include Impl

  module Readable : Blockstore.Readable with type t = Impl.t = Impl

  module Writable : Blockstore.Writable with type t = Impl.t = Impl
end

module Overlay_blockstore
    (Top : Blockstore.Readable)
    (Bottom : Blockstore.Readable) =
struct
  module Impl = Overlay_blockstore.Make (Top) (Bottom)
  include Impl

  module Readable : Blockstore.Readable with type t = Impl.t = Impl
end

module Cache_blockstore (Bs : Blockstore.Writable) = struct
  module Impl = Cache_blockstore.Make (Bs)
  include Impl

  module Readable : Blockstore.Readable with type t = Impl.t = Impl

  module Writable : Blockstore.Writable with type t = Impl.t = Impl
end
