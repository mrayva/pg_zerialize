# Vendored glaze provenance

The vendored source is synchronized with:

- Repository: <https://github.com/stephenberry/glaze>
- Tag: `v8.0.0`
- License: MIT (`LICENSE` in this directory)

## Why glaze is vendored (not apt) and pruned

`vendor/zerialize/include/zerialize/protocols/beve.hpp` needs glaze's
zero-copy "lazy BEVE" reader (`glaze/beve/lazy.hpp`), which zerialize itself
pins at v8.0.0. The apt package `libglaze-dev` (5.5.4-1 on Ubuntu
24.04/25.10 as of 2026-08) is a materially older release that predates that
reader entirely -- it has no `glaze/beve/lazy.hpp` at all -- so it can't be
substituted the way `libjsoncons-dev` was for BSON. Glaze has to be vendored
from source instead, same as `vendor/zerialize` itself.

Glaze v8.0.0's full source is 255 headers / 4.9MB, but
`glaze/beve/lazy.hpp` only transitively includes 63 of them (1.5MB) --
confirmed via `g++ -std=c++23 -H` dependency tracing against a clean
checkout. Only those 63 files are vendored here, following the precedent
set by `vendor/zerialize/UPSTREAM.md`: "compare the fork against this
directory and reapply these changes deliberately. Do not replace the
vendored tree wholesale."

## Local patch

- `include/glaze/core/chrono.hpp` (`write_iso_time_point`): two calls read
  `floor<days>(...)` / `floor<Duration>(...)` unqualified, relying on
  `using namespace std::chrono;` earlier in the function. PostgreSQL's
  `utils/datetime.h` (pulled in by `pg_zerialize.cpp` before any of this)
  declares `extern PGDLLIMPORT const char *const days[]` at global scope --
  a real symbol, not a macro, so `#undef` doesn't apply the way it does for
  `INVALID` (see `pg_zerialize.cpp`'s own `#undef INVALID` comment). With
  both `::days` (PostgreSQL's array) and `std::chrono::days` (the duration
  type, injected by the using-directive) visible, unqualified `days`
  resolves to PostgreSQL's array instead of the duration type, and
  `floor<days>` fails to parse as a template-id at all -- not just picks
  the wrong overload. Fixed by fully qualifying both call sites:
  `std::chrono::floor<std::chrono::days>(value)` and
  `std::chrono::floor<Duration>(...)` (already qualified on `floor`, kept
  as-is -- `Duration` is a local alias, not at risk). Confirmed via a
  repo-wide grep that no other vendored file references `days` or
  PostgreSQL's other datetime.h globals (`months`) unqualified. When
  updating glaze, recheck this: grep the newly-vendored files for bare
  `days`/`months`/`weeks`/`years` inside any `using namespace std::chrono`
  scope.

## Updating

To bump to a newer glaze release:

1. Clone the new tag somewhere scratch, e.g. `git clone --depth 1 --branch
   vX.Y.Z https://github.com/stephenberry/glaze.git`.
2. Recompute the used-header set: compile a translation unit that only
   `#include <glaze/beve/lazy.hpp>` with `g++ -std=c++23 -I<scratch>/include
   -H -c minimal.cpp -o /dev/null 2>&1 | grep '<scratch-path>' | awk '{print
   $NF}' | sort -u` and copy exactly that file list into
   `vendor/glaze/include/`, preserving directory structure. The set can
   change release to release (glaze's internal includes aren't stable), so
   don't just diff-and-patch the old file list.
3. Confirm the full extension still builds and `make installcheck` passes
   under `-std=c++23` before committing.
