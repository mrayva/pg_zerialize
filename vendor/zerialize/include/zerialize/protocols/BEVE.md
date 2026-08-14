# BEVE Protocol

`BEVE` (Binary Efficient Versatile Encoding, [beve-org/beve](https://github.com/beve-org/beve)) is a
self-describing binary format from the same design lineage as [glaze](https://github.com/stephenberry/glaze).
zerialize adapts it rather than reimplementing it from scratch:

- **Reading** wraps glaze's `glz::lazy_beve_document` / `glz::lazy_beve_view`
  (`<glaze/beve/lazy.hpp>`) — a non-owning, bounds-checked, zero-copy walk over raw BEVE
  bytes. We adapt its API to zerialize's `Reader` concept instead of hand-rolling BEVE's
  tag-byte parsing and bounds checking a second time.
- **Writing** is a small hand-rolled byte-appender with **no runtime dependency on glaze** —
  see "Why the writer doesn't use glaze" below.

Implementation: `include/zerialize/protocols/beve.hpp`

## Dependency and C++ standard

BEVE support is **opt-in** (`ZERIALIZE_ENABLE_BEVE`, default `OFF`), unlike zerialize's other
protocol options. glaze is vendored via `FetchContent` (pinned to `v8.0.0`), and its own
`CMakeLists.txt` unconditionally requires `cxx_std_23` on its INTERFACE target. Linking it into
zerialize's own INTERFACE target propagates that requirement to any consumer that enables the
option — so enabling BEVE raises the C++ standard your project is compiled with from zerialize's
normal C++20 baseline to C++23. This is why the option defaults off: every other protocol here
builds under C++20.

## Data model

Same least-common-denominator model as the other protocols: `null`, `bool`, `int64`, `uint64`,
`float64`, `string`, `array`, `object` (string keys only), and `blob`.

Blobs are represented as a BEVE **typed array** of `uint8` — a homogeneous, unaligned byte run
(header + compressed size + raw bytes, no per-element tags). `isBlob()`/`isArray()` are mutually
exclusive: a typed `uint8` array is reported as a blob, never as a generic array, matching the
same disambiguation `zera.hpp` uses for its own blob representation.

## Wire format (for reference)

Every value starts with a 1-byte header; bit 0 is the least-significant bit. Bits 0-2 are the
type:

| bits 0-2 | type |
|---|---|
|`000`| null / boolean |
|`001`| number |
|`010`| string |
|`011`| object |
|`100`| typed array |
|`101`| generic array |
|`110`| extension |

For `null`/`boolean`: `0x00` = null, `0x08` = false, `0x18` = true.

For `number` and `typed array`, bits 3-4 are a category (`00` float, `01` signed, `10` unsigned,
and — typed arrays only — `11` boolean/string) and bits 5-7 are a byte-width index
(`0..3` → `1,2,4,8` bytes; `4..7` → `16,32,64,128` bytes, not used by this implementation — see
Limitations).

Container sizes, string lengths, and typed-array element counts use a compressed unsigned
integer: the low 2 bits of the *first* byte select a total width (`00`→1 byte, `01`→2 bytes,
`10`→4 bytes, `11`→8 bytes, little-endian), and the value occupies the remaining bits, i.e.
`value = (word >> 2)` once the word of the selected width has been read.

Objects are `HEADER | SIZE | KEY₀ VALUE₀ ... KEYₙ VALUEₙ`; keys omit their own header (the
object header already says "string keys") and are encoded like a string's payload
(`SIZE | UTF-8 bytes`) with no leading type byte. Generic arrays are `HEADER | SIZE | VALUE₀ ...
VALUEₙ`, each value self-tagged. Typed arrays are `HEADER | SIZE | DATA`, with `DATA` a tight,
unaligned run of `count` fixed-width elements (or, for typed string arrays, `count`
length-prefixed strings) and no per-element tags.

This is verified against the reference implementation empirically, not just against the spec
prose — see "A note on the spec text" below.

## Why the writer doesn't use glaze

glaze's write path is reflection/trait-driven (`glz::write_beve<T>`); there's no low-level
streaming writer equivalent to the lazy reader. The natural glaze-based alternative would be to
build a `glz::generic` (glaze's dynamic JSON-like tree type) from zerialize's `Writer` calls and
call `glz::write_beve()` on it once at `finish()`.

That was tried and rejected: `glz::generic`'s array member is `std::vector<glz::generic_json<...>>`
— a heterogeneous, runtime-tagged container — and glaze only picks BEVE's "typed array"
wire encoding for *statically*-typed homogeneous containers (e.g. a struct field of type
`std::vector<uint8_t>`). Writing a blob through a `glz::generic` tree therefore always produces a
BEVE **generic array** of individually-tagged small integers (confirmed by inspecting the actual
output bytes), not a typed `uint8` array — which would silently fail to round-trip through this
library's own `isBlob()`/`asBlob()`, and would be far larger on the wire (9 bytes per byte instead
of 1).

Since zerialize's `begin_array(n)`/`begin_map(n)` already receive the element count up front, a
single hand-written streaming pass (the same shape as `protocols/msgpack.hpp`'s writer) is
straightforward and keeps the wire encoding under our control. Integers are written at the
smallest width that round-trips exactly (mirroring `msgpack-c`'s own behavior); doubles are
always written as `float64`.

## A note on the spec text

The upstream README's prose description of the format has a couple of small inaccuracies
relative to glaze's own reference implementation (e.g. it describes the boolean true/false bit
and the typed-array boolean-vs-string subtype bit as being one position off from where the
reference code actually reads them). This implementation was cross-checked against
glaze's `include/glaze/beve/lazy.hpp` and `header.hpp` directly, and against the actual bytes
produced by `glz::write_beve()` for representative values, rather than against the prose alone.

## Known v1 limitations

- **Extended numeric widths** (`bfloat16`, `float16`, `float128`, `int128`/`uint128`) are not
  exposed by zerialize's scalar accessors (which top out at 64-bit ints and `double`) and are not
  written by this implementation. Reading foreign BEVE data that uses them is not supported.
- **Extensions** (data delimiter, matrices, complex numbers) are not implemented. BEVE's matrix
  extension (row/col-major + extents + typed-array payload) would be a natural future zero-copy
  path for `zerialize`'s `tensor/` module, but that's follow-up work, not v1 scope.
- **Integer-keyed objects** are not written, and are not readable (only string-keyed objects are
  supported, matching zerialize's own data model).
- **Packed boolean typed arrays** (`std::vector<bool>`-style bit-packed storage) can be read as a
  container (`isArray()`, `arraySize()`) but individual elements cannot be random-accessed —
  glaze's own lazy reader doesn't support indexing into them either ("users should use size() and
  iteration instead"). This implementation never *writes* this form (`zvec`/`zmap` always produce
  self-tagged generic-array elements), so it only matters when reading BEVE bytes produced by
  another implementation.

## Interop and translation

Beve implements the same `Reader`/`Writer` surface as the other protocols:

- Serialize with `serialize<zerialize::Beve>(...)`
- Translate to/from other protocols with `translate<OtherProto>(beve_reader)` /
  `translate<zerialize::Beve>(other_reader)`
