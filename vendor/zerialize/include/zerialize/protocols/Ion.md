# Ion (binary) Protocol

`Ion` ([amazon-ion.github.io/ion-docs](https://amazon-ion.github.io/ion-docs)) is implemented
entirely hand-rolled — **no runtime dependency on `ion-c` or `ion-rust`**. Both the reader and the
writer were designed after cloning and reading `ion-c` (the reference C implementation) and
`ion-rust` (Amazon's Rust implementation) directly, and every wire-format detail below was
cross-checked against real bytes produced by `ion-c`'s own writer before being relied on here —
not assumed from the spec text alone.

Implementation: `include/zerialize/protocols/ion.hpp`

## Why hand-rolled, and why not `ion-c`

`ion-c`'s reader is a single, stateful, forward-only cursor (`ion_reader_next`/`step_in`/
`step_out`) — there's no zero-copy view type, and no DOM layer either. Wrapping it would mean
either giving up zero-copy (materialize a full tree up front) or exposing a `Deserializer` whose
`operator[]` mutates shared cursor state — which fails outright for ordinary generic code: C++
does not guarantee the evaluation order of a function call's arguments, so code that calls
`operator[]` on the same `const Deserializer&` multiple times as part of one expression (a
completely ordinary pattern — see any consumer that extracts several named fields from one parsed
document) would get a nondeterministic result if `operator[]` had side effects on shared state.

`ion-rust`'s `lazy` module (`src/lazy/`) proves the alternative is buildable: its `LazyStruct`/
`LazyValue` are `Copy`, documented as "immutable; fields can be read any number of times," and its
`BytesRef`/`StrRef` are genuine borrowed references into the source buffer. The piece that makes
this possible — an `EncodingContext { symbol_table: Rc<SymbolTable> }` resolved once and shared via
lightweight references — is the architecture this implementation ports to C++: a `shared_ptr<const
Context>` holding the backing buffer and the resolved local symbol table, referenced by every small,
copyable, `const`-safe value handle. This is the same lifetime pattern `beve.hpp` uses for the same
underlying reason (a bare pointer back to a document that could move or be destroyed on copy is
exactly the class of bug that produced a real heap-buffer-overflow in glaze's own lazy reader — see
`BEVE.md`).

## Wire format

Every value starts with a type descriptor byte: high nibble = type (0-15), low nibble = length
(`0`-`13` literal, `14`→a VarUInt length follows, `15`→null of this type). A **VarUInt** is a
big-endian sequence of 7-bit groups where the *last* byte (not the first) has its high bit set —
the opposite convention from protobuf/LEB128's "high bit means more bytes follow." This, and the
fact that Ion is **big-endian** throughout (ints, floats — the opposite of BEVE's little-endian),
were both verified by hand-predicting a 2-byte VarUInt encoding and a magnitude encoding before
running the real thing and confirming an exact match, not assumed.

| type | meaning |
|---|---|
|`0`| null / NOP padding (low `0`-`13` = NOP pad of that many bytes, low `15` = `null.null`) |
|`1`| bool — value embedded directly in the low nibble (`0x10`/`0x11`), no separate payload |
|`2`/`3`| positive/negative int — big-endian magnitude, minimal bytes, sign via type not two's-complement; `0` is always encoded as type `2` with zero-length magnitude |
|`4`| float — length `0` (=0.0), `4`, or `8`, big-endian IEEE-754 |
|`5`/`6`| decimal / timestamp — not decoded as scalars (see limitations) |
|`7`| symbol — payload is a VarUInt symbol ID (SID), resolved via the symbol table |
|`8`| string — raw UTF-8 |
|`9`| clob — not decoded as a scalar (see limitations) |
|`10`| blob — raw bytes (zerialize `blob`) |
|`11`/`12`| list / sexp — same physical layout; both map to zerialize `array` |
|`13`| struct (zerialize `map`) — `(VarUInt field SID, value)` pairs, **length-prefixed, no terminator byte** (unlike BSON) |
|`14`| annotation wrapper — `VarUInt annot_length | annotation SIDs (VarUInt each) | wrapped value` |

Because every type's header generically encodes `content_off`/`content_len`, `value_end`/skip never
needs a per-type switch the way BSON's does — one `read_header` call bounds any value, scalar or
container alike.

## Symbol resolution

Struct field names and symbol values are VarUInt SIDs, not inline text — the one piece of real
complexity Ion has that BSON/BEVE don't. Resolution:

- **SIDs 1-9** are the fixed system symbol table: `$ion`, `$ion_1_0`, `$ion_symbol_table`, `name`,
  `version`, `imports`, `symbols`, `max_id`, `$ion_shared_symbol_table`. **A user-chosen field name
  can silently collide with one of these** — confirmed empirically: a test struct using the field
  name `"name"` got mapped to system SID 4 rather than a new local entry. The reader checks SIDs
  1-9 against this fixed table before ever treating a SID as needing a local-table lookup; the
  writer does the same check before minting a new local symbol, so re-using a system symbol name as
  a field costs nothing extra on the wire.
- **SIDs 10+** are local symbols, defined by a struct annotated with system SID 3
  (`$ion_symbol_table`) containing a `symbols` (SID 7) field — a list of new symbol strings,
  assigned SIDs sequentially starting at 10. This definition is resolved once, incrementally,
  during the same top-level forward scan needed anyway to find the root value
  (`scan_and_build_symtab`) — reading different fields of the same struct afterward costs nothing
  extra symbol-table-wise, which is the key property that makes laziness and correct symbol
  resolution non-conflicting (the same property `ion-rust`'s design demonstrates).
- **Shared/imported symbol tables** (an `imports` field referencing an external, catalog-based
  table by name+version) are **not supported**. This implementation follows only local tables and
  the "append to current table" convention (`imports` value is the symbol `$ion_symbol_table`
  itself); a document whose `imports` field is a non-empty list throws `DeserializationError`
  rather than silently misreading it.

The writer mints local symbols on demand as `key()` is called, reusing the same SID for a
repeated field name (tracked in an id map) and checking the 9 system names first. The resulting
local symbol table is assembled once and prepended — as its own top-level entry, before the actual
value — at `finish()`, since by then every new symbol used is already known in the order it was
first seen.

## Container-length writing

Unlike BEVE/MsgPack (which receive an element *count* up front via `begin_array(n)`) or BSON
(whose length field is a fixed 4 bytes, patchable in place), Ion's length prefix is a **byte
count**, and its own encoding width is variable (inline vs. VarUInt) depending on the value being
encoded — which isn't known until the container's content has actually been written. Rather than
risk relying on non-minimal VarUInt padding to patch a reserved-width slot in place, the writer
builds each container's content into its own buffer (a stack, pushed at `begin_array`/`begin_map`,
popped and spliced into the parent once its size is known at `end_array`/`end_map`), then emits
the correctly-sized header immediately before it.

## No root-value ambiguity (unlike BSON)

Every Ion value is self-describing at its own offset — including containers — so, unlike BSON,
there's no format-inherent ambiguity between "the root is a document" and "the root is an array."
`serialize<Ion>(zvec(...))` round-trips as `isArray()`, exactly as it does for every other
non-BSON backend. Ion gets the same generic `test_protocol_dsl<P>()`/`test_dynamic_serialization<P>()`
coverage every other backend gets — no bespoke carve-out was needed.

## Known v1 limitations

- **`decimal` and `timestamp`** — Ion's two headline types over BSON/CBOR (arbitrary-precision
  base-10 decimal; timestamp with preserved authorial precision and explicit UTC offset) are
  **not exposed as scalars**. They're correctly skipped (so traversing a struct or list containing
  one doesn't break), matching the posture `BSON.md` takes for MongoDB-specific types. Exposing
  them meaningfully would require new primitives on zerialize's `Writer`/`Reader` concept itself
  (`decimal()`/`timestamp()`), which is a larger, separate design decision — not attempted here.
- **`clob`** — same treatment as decimal/timestamp: skippable, not exposed (trivially could be,
  since it's just raw bytes like blob, but kept distinct rather than silently conflating two
  semantically different types).
- **Shared/imported symbol tables** — see above; throws clearly rather than misreading.
- **Arbitrary-precision `int`** — Ion's `int` type has no fixed width. Values whose magnitude
  needs more than 8 bytes throw `DeserializationError` from `asInt64()`/`asUInt64()`, the same
  posture as every other 64-bit-bounded accessor in this library.
- **`sexp` and `list` are not distinguished** — both read back as zerialize `array`, since
  zerialize's data model has only one array-like concept and the two are physically identical on
  the wire.
- A stream's Binary Version Marker is required at the very start; mid-stream BVMs (used to reset
  symbol table state) are not handled, since zerialize documents are single root values, not
  multi-value Ion "datagrams."

## Interop and translation

Ion implements the same `Reader`/`Writer` surface as the other protocols:

- Serialize with `serialize<zerialize::Ion>(...)`
- Translate to/from other protocols with `translate<OtherProto>(ion_reader)` /
  `translate<zerialize::Ion>(other_reader)`
