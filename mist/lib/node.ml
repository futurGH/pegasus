type tree_entry =
  { p: int (* length of prefix shared with previous key *)
  ; k: bytes (* remainder of key after the prefix *)
  ; v: Cid.t (* CID of value *)
  ; t: Cid.t option (* right-hand subtree pointer *) }

type node_data =
  { l: Cid.t (* leftmost subtree pointer *)
  ; e: tree_entry list (* list of tree entries *) }
