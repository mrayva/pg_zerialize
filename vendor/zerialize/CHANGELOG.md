# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.1] - 2026-08-02

A security/correctness hardening pass across every protocol backend, found and
verified with AddressSanitizer/UndefinedBehaviorSanitizer, plus a build
reliability fix and dependency pinning.

### Security

- **CBOR**: fixed three heap-buffer-overflow reads reachable from
  malformed/truncated input:
  - `asDouble()`/`asFloat()` read the float16/32/64 payload with no bounds
    check.
  - `contains()`'s map-key fast path built a `string_view` from unchecked
    header info instead of the bounds-checked `asString()`.
  - `KeysView` (`mapKeys()`) carried its own private copy of the
    header-parsing logic that omitted nearly all of the bounds checks the
    canonical implementation has.
- **MessagePack**: fixed the same class of bug, more pervasively — the
  `str_info()`, `bin_info()`, `arr_info()`, and `map_info()` header-parsing
  helpers read multi-byte length prefixes with no bounds checking at all, and
  `mp_skip()` validated headers but not payload lengths for string/binary
  markers, letting truncated input produce an out-of-range offset passed to
  `std::span::subspan()` (undefined behavior, not a safe throw).
- **FlexBuffers**: fixed an out-of-bounds read in `string()`/`key()` — both
  passed `std::string_view::data()/size()` straight to flatbuffers APIs that
  read one byte past the end expecting a trailing NUL (a guarantee
  `std::string_view`, unlike `std::string`, doesn't provide).
- **Tensor deserialization (Eigen)**: added an overflow check to
  `asEigenMatrixView()`'s `rows * cols * sizeof(T)` byte-size validation. A
  crafted tensor shape could previously wrap the check on 64-bit `size_t` and
  pass validation with a mismatched buffer, causing an uncontrolled
  allocation / crash instead of a clean `DeserializationError`.

### Fixed

- `zera::RootSerializer::finish()` and four related call sites (`zera`'s
  `contains()`/`operator[]`, and several tensor-deserialization `memcpy`s in
  `eigen.hpp`/`xtensor.hpp`) called `memcpy`/`memcmp` with a possibly-null
  source pointer from an empty container (e.g. a zero-element tensor, a
  0-row/0-column matrix, or an empty-string map key) — technically undefined
  behavior even for a zero-length copy. Guarded all of them, matching the
  convention already used elsewhere in these files.
- `fixed_string::c_str()` failed to compile whenever instantiated (called
  `.data()` on a raw `char[N]` array instead of returning it directly).
- `MsgPackRootSerializer` had no copy/move control despite owning a raw
  `msgpack_sbuffer` pointer, risking a double-free if ever copied; added
  explicit copy-deletion and a correct move that re-anchors the internal
  packer state.
- `json::RootSerializer` leaked its `yyjson_mut_doc*` if a `Writer` call (or
  the source `Reader` being translated from, via `translate<JSON>()`) threw
  before `finish()` was reached — happened on every failed deserialization
  translated to JSON, not just contrived cases.
- Fixed a header-only flatbuffers build failure that only manifested on a
  genuinely fresh build (`undefined reference to
  flatbuffers::ClassicLocale::instance_`) — flatbuffers' locale-handling code
  needs a translation unit this project never compiles; forced
  `FLATBUFFERS_LOCALE_INDEPENDENT=0` so the affected headers fall back to
  plain `strtod`/`strtoll` instead.
- `zbuffer.hpp` was missing `#include <memory>` (`ManagedPtr` uses
  `std::unique_ptr`), only surfacing when building this repo standalone.

### Changed

- Pinned `flatbuffers` and `msgpack-c`, which were previously tracking
  floating `master`/`c_master` branches, to explicit release tags
  (`v25.12.19`, `c-7.0.1`) for reproducible builds.
