# Vendored zerialize provenance

The vendored source is synchronized with:

- Repository: <https://github.com/mrayva/zerialize> (fork of
  <https://github.com/colinator/zerialize>)
- Commit: `32d9d9447c9ce725ba0ea9d1a5d25005066a0cd8`
- Commit date: 2026-08-04

The fork carries these bug fixes on top of upstream commit `aedaaf2` (the
commit this vendor tree previously tracked):

- OOB reads in the MessagePack parser (`mp_skip`, `str_info`, `bin_info`,
  `arr_info`, `map_info`) on truncated input.
- OOB reads in the CBOR parser (float payload, map-key, and container
  truncation checks) and in the Flex string/key writer (non-null-terminated
  `string_view` passed to `flexbuffers::Builder::String`/`Key`).
- UB from a null `memcpy` source in `zera::RootSerializer::finish()` when the
  environment or arena buffer is empty, plus null-pointer `memcmp`/`memcpy`
  guards elsewhere in `zera`, `eigen`, and `xtensor`.
- `fixed_string::c_str()` returning the wrong pointer.
- An exception-safety leak in `json::RootSerializer`.

`pg_zerialize` carries local serialization hot-path changes in:

- `include/zerialize/protocols/flex.hpp`: disables key/string sharing.
- `include/zerialize/protocols/msgpack.hpp`: replaces the msgpack-c packer
  writer with a direct raw-append encoder, and adds preencoded map/key
  writers.
- `include/zerialize/protocols/zera.hpp`: adds a preencoded key writer.
- `include/zerialize/zbuffer.hpp`: includes `<memory>` for owned buffers
  (this fix landed upstream in the fork; kept here for clarity).
- `include/zerialize/protocols/cbor.hpp`: replaces the jsoncons-based
  `RootSerializer`/`Serializer` with a direct RFC 8949 byte encoder. Upstream's
  CBOR writer requires the jsoncons headers, which are not vendored anywhere
  in this tree and have no apt package, so every build of this extension
  failed before this change (confirmed via this repo's CI history). The
  hand-rolled encoder removes that dependency entirely; `CborDeserializer`
  (used by `cbor_to_jsonb`) is untouched.

When updating, compare the fork against this directory and reapply these
changes deliberately. Do not replace the vendored tree wholesale.
