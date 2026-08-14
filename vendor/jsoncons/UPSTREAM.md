# Vendored jsoncons provenance

The vendored source is synchronized with:

- Repository: <https://github.com/danielaparker/jsoncons>
- Tag: `v1.3.2`
- License: Boost Software License 1.0 (`LICENSE` in this directory)

## Why jsoncons is vendored (not apt) and pruned

`vendor/zerialize/include/zerialize/protocols/bson.hpp`'s writer wraps
`jsoncons::bson::bson_bytes_encoder`. This extension previously depended on
the apt package `libjsoncons-dev` for it -- that was wrong. `libjsoncons-dev`
was verified against this development box (Ubuntu 25.10, "questing") but
never against CI's actual runner image, and it turns out the package doesn't
exist for Ubuntu 24.04 ("noble", what `ubuntu-latest` currently resolves to)
or 25.04 ("plucky") at all -- only 25.10 and later ship it, in `universe`.
CI's "Install dependencies" step has been failing with `E: Unable to locate
package libjsoncons-dev` on every run since the commit that added it (see
git history around `.github/workflows/ci.yml`), unnoticed because nothing
was watching CI status at the time. Rather than wait for `ubuntu-latest` to
eventually catch up (which won't happen for a long time -- GitHub Actions
tracks Ubuntu LTS releases, and 24.04 will be `ubuntu-latest` until 26.04
stabilizes), jsoncons is vendored from source instead, same reasoning as
`vendor/glaze` (see `vendor/glaze/UPSTREAM.md`): a build dependency this
extension actually needs shouldn't be at the mercy of what a particular CI
image's apt repos happen to carry.

jsoncons v1.3.2's full source is 152 headers / 3.5MB, but the three headers
`bson.hpp`'s writer includes (`jsoncons/json.hpp`,
`jsoncons_ext/bson/bson.hpp`, `jsoncons_ext/bson/bson_type.hpp`)
transitively pull in 63 of them -- confirmed via `g++ -std=c++23 -H`
dependency tracing against a clean checkout, the same method used for
`vendor/glaze`. Only those 63 files are vendored here, following the same
precedent: compare upstream against this directory and reapply changes
deliberately; don't replace the vendored tree wholesale.

Note the pulled-in set includes jsoncons' BSON *decoder*
(`bson_reader.hpp`, `bson_cursor.hpp`, `decode_bson.hpp`) even though this
extension only uses the encoder -- `jsoncons_ext/bson/bson.hpp` itself
`#include`s both unconditionally, so `-H` tracing (which reflects what
actually gets compiled into this translation unit) picks them up. Not
pruned further than that; matches what the compiler already builds today.

## Local patch

None. Vendored verbatim.

## Updating

To bump to a newer jsoncons release:

1. Clone the new tag somewhere scratch, e.g. `git clone --depth 1 --branch
   vX.Y.Z https://github.com/danielaparker/jsoncons.git`.
2. Recompute the used-header set: compile a translation unit containing
   exactly `bson.hpp`'s three includes (`jsoncons/json.hpp`,
   `jsoncons_ext/bson/bson.hpp`, `jsoncons_ext/bson/bson_type.hpp`) plus a
   minimal use of `bson_bytes_encoder` (so nothing gets optimized/pruned
   away by the preprocessor never reaching it), with `g++ -std=c++23
   -I<scratch>/include -H -fsyntax-only minimal.cpp 2>&1 | grep
   '<scratch-path>' | sed 's#.*<scratch-path>/include/##' | sort -u`, and
   copy exactly that file list into `vendor/jsoncons/include/`, preserving
   directory structure. The set can change release to release, so don't
   just diff-and-patch the old file list.
3. Confirm the full extension still builds and `make installcheck` passes
   before committing.
