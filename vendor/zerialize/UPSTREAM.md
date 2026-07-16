# Vendored zerialize provenance

The vendored source is synchronized with:

- Repository: <https://github.com/colinator/zerialize>
- Commit: `aedaaf2e585dbc0a7f239bca33fd2a6595ed47df`
- Commit date: 2025-12-22

`pg_zerialize` carries local serialization hot-path changes in:

- `include/zerialize/protocols/flex.hpp`: disables key/string sharing.
- `include/zerialize/protocols/msgpack.hpp`: adds raw append and pre-encoded
  map/key writers.
- `include/zerialize/protocols/zera.hpp`: adds pre-encoded key writers.
- `include/zerialize/zbuffer.hpp`: includes `<memory>` for owned buffers.

When updating, compare upstream against this directory and reapply these
changes deliberately. Do not replace the vendored tree wholesale.
