# Vendored zerialize provenance

The vendored source is synchronized with:

- Repository: <https://github.com/mrayva/zerialize> (fork of
  <https://github.com/colinator/zerialize>)
- Commit: `32d04de942b4b7c87dba358d97d7346ef9769cfd` (upstream: `Add
  elements() to fix O(n^2) array decoding for consumers`)
- Commit date: 2026-08-14

`f0f07a1` adds one thing on top of `13f6383`: `mapEntries()` on
`MsgPackDeserializer`/`IonDeserializer`/`BsonDeserializer`/`BeveDeserializer`
(single-pass key+value iteration, vs. `mapKeys()` + `operator[](key)`'s
O(n) re-scan per lookup). Found while auditing this extension's own
`reader_value_to_json`/`msgpack_reader_to_json` (`pg_zerialize.cpp`) for an
O(n^2) map-decode pattern in a per-row hot path, fixed locally here first
(verified: all 28 REGRESS tests, ~3.5x on a 200-key object), then ported
upstream and pulled back in as that bump.

`32d04de` does the same thing for array decoding: adds `elements()` (single-
pass element iteration vs. `arraySize()` + `operator[](idx)`'s O(n) re-scan
per lookup) to the same four deserializers. Same discover-fix-port-back
cycle (verified: ~2.9x on a 300-element array; BEVE typed numeric/string
arrays and empty/nested/heterogeneous arrays spot-checked across all four
formats). For BEVE, `elements()` only speeds up generic (heterogeneous)
arrays -- numeric typed arrays already had true O(1) `operator[]` access
via glz's fixed-element-width pointer math, and glz's own iterator doesn't
tag typed-array elements it yields, so `elements()` falls back to
per-index access for those (no worse than before).

So, once again: `ion.hpp`/`bson.hpp`/`beve.hpp` are vendored verbatim with
no local patch, and `msgpack.hpp`'s reader-side `mapEntries()`/`elements()`
additions are upstream-identical too (its writer remains locally patched,
see below, unaffected by either change since both are reader-only).

Bumped from the previous pin, `32d9d9447c9ce725ba0ea9d1a5d25005066a0cd8`
(2026-08-04): that bump added `include/zerialize/protocols/ion.hpp` and
`bson.hpp` (plus their `Ion.md`/`BSON.md` docs) to this tree — both are new
files upstream, so the patches below (all against `cbor.hpp`/`msgpack.hpp`/
`zera.hpp`/`flex.hpp`/`zbuffer.hpp`) needed no rework: `git diff 32d9d944
13f6383 --stat` in the fork shows zero changes to any of those five files
between the two pins. `include/zerialize/protocols/beve.hpp` was
deliberately NOT vendored in that bump — it requires a C++23 compiler
(this extension targeted C++20 at the time) and pulls in glaze, a separate,
larger change taken on its own afterward: see the `beve.hpp`/glaze entry
below and `vendor/glaze/UPSTREAM.md`.

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
  `RootSerializer`/`Serializer` with a direct RFC 8949 byte encoder. At the
  time this was written, upstream's CBOR writer's jsoncons dependency
  wasn't vendored anywhere in this tree and (it was believed) had no apt
  package, so every build of this extension failed before this change
  (confirmed via this repo's CI history). The hand-rolled encoder removes
  that dependency entirely; `CborDeserializer` (used by `cbor_to_jsonb`) is
  untouched. Note: as of the 2026-08-13 bump, `libjsoncons-dev` turned out
  to be apt-installable after all (verified on Ubuntu 24.04/25.10) and is
  now a build dependency anyway for `bson.hpp`'s writer (see below) — but
  this hand-rolled CBOR encoder is being left as-is rather than reverted,
  since it works and reverting isn't in scope here.
- `include/zerialize/protocols/bson.hpp` and `ion.hpp`: vendored verbatim,
  unmodified, from the fork. `ion.hpp` is genuinely dependency-free.
  `bson.hpp`'s reader is hand-rolled and dependency-free; its writer wraps
  `jsoncons::bson::bson_bytes_encoder`, so `libjsoncons-dev` is now a build
  dependency of this extension (see `.github/workflows/ci.yml` and
  `README.md`'s Requirements section).
- `include/zerialize/protocols/beve.hpp`: vendored verbatim, unmodified,
  from the fork (2026-08-14). Wraps glaze's zero-copy `lazy_beve_document`/
  `lazy_beve_view` for the reader; the writer is zerialize's own hand-rolled
  code, no glaze dependency there. Requires `-std=c++23` (this extension's
  `PG_CPPFLAGS` was bumped from `-std=c++20`) and glaze v8.0.0, vendored
  under `vendor/glaze/` — see `vendor/glaze/UPSTREAM.md` for why that's
  vendored from source rather than via apt, and why only a pruned subset of
  glaze is included.

When updating, compare the fork against this directory and reapply these
changes deliberately. Do not replace the vendored tree wholesale.
