// Amazon Ion (binary) protocol — hand-rolled reader and writer, no runtime
// dependency on ion-c or ion-rust.
//
// Every wire-format detail below (type descriptor bit layout, VarUInt
// encoding, the local symbol table's exact struct shape, big-endian
// magnitude/float encoding) was verified against real bytes produced by
// ion-c's own writer before this file was written, not assumed from the
// spec text alone — see Ion.md for the decode walkthrough and the two
// details that would have been easy to get wrong from memory (VarUInt's
// continuation-bit convention is the *opposite* of protobuf/LEB128's, and
// big-endian is the opposite of BEVE's little-endian).
//
// Design, in short (see Ion.md for the full rationale):
//   - Every value is self-describing at its own offset (a leading type
//     descriptor byte), same as BEVE/CBOR/MsgPack — unlike BSON, where a
//     struct's parent held the type separately.
//   - Field names and "symbol" values are symbol IDs (SIDs), not inline
//     text. Resolving one requires a small system table (SIDs 1-9, fixed)
//     plus a local table built once, incrementally, during the same
//     top-level forward scan needed anyway to find the root value — see
//     `scan_and_build_symtab`. This mirrors the architecture ion-rust's
//     `lazy` module uses (a small, Copy-able value handle plus a shared,
//     already-resolved symbol-table context), ported to C++ as a
//     shared_ptr<Context>, the same pattern beve.hpp uses for the same
//     underlying reason (subviews must not outlive/dangle the context they
//     reference across copies).
//   - Shared/imported symbol tables (catalog-based, cross-document) are not
//     supported — this implementation only follows local symbol tables and
//     the "append to current table" convention; a document that imports a
//     shared table throws DeserializationError rather than silently
//     misreading it. See Ion.md's "Known v1 limitations".
//   - decimal/timestamp/clob are skippable (so traversal never breaks on
//     them) but not exposed as scalars — same posture as BSON.md's
//     MongoDB-specific-type carve-outs.
#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include <span>
#include <memory>
#include <stdexcept>
#include <sstream>
#include <limits>
#include <unordered_map>
#include <array>

#include <zerialize/zbuffer.hpp>
#include <zerialize/errors.hpp>

namespace zerialize {
inline void ensure(bool cond, const char* msg) {
    if (!cond) throw DeserializationError(msg);
}

namespace ionb {

// ===== shared wire-format constants ==========================================

// Type descriptor high nibble.
inline constexpr std::uint8_t kTypeNull       = 0;
inline constexpr std::uint8_t kTypeBool       = 1;
inline constexpr std::uint8_t kTypePosInt     = 2;
inline constexpr std::uint8_t kTypeNegInt     = 3;
inline constexpr std::uint8_t kTypeFloat      = 4;
inline constexpr std::uint8_t kTypeDecimal    = 5;
inline constexpr std::uint8_t kTypeTimestamp  = 6;
inline constexpr std::uint8_t kTypeSymbol     = 7;
inline constexpr std::uint8_t kTypeString     = 8;
inline constexpr std::uint8_t kTypeClob       = 9;
inline constexpr std::uint8_t kTypeBlob       = 10;
inline constexpr std::uint8_t kTypeList       = 11;
inline constexpr std::uint8_t kTypeSexp       = 12;
inline constexpr std::uint8_t kTypeStruct     = 13;
inline constexpr std::uint8_t kTypeAnnotation = 14;
inline constexpr std::uint8_t kTypeReserved   = 15;

// System symbol table (SIDs 1-9, fixed by the Ion 1.0 spec). Index 0 is a
// placeholder so `kSystemSymbolText[sid]` works directly; SID 0 itself is
// the spec's "symbol zero" and always resolves to empty text.
inline constexpr std::array<std::string_view, 10> kSystemSymbolText = {
    std::string_view{},
    "$ion", "$ion_1_0", "$ion_symbol_table", "name", "version",
    "imports", "symbols", "max_id", "$ion_shared_symbol_table",
};
inline constexpr std::uint32_t kSidIonSymbolTable = 3;
inline constexpr std::uint32_t kSidImports        = 6;
inline constexpr std::uint32_t kSidSymbols        = 7;
inline constexpr std::uint32_t kFirstLocalSid     = 10;

inline constexpr std::array<std::uint8_t, 4> kBinaryVersionMarker = {0xE0, 0x01, 0x00, 0xEA};

// ===== low-level codec (shared by reader and writer) ==========================

// VarUInt: big-endian sequence of 7-bit groups; the *last* byte (not the
// first) has its high bit set. This is the opposite convention from
// protobuf/LEB128's "high bit means more bytes follow" — verified against
// real ion-c output (a 200-byte string's length encoded as `01 c8`) before
// relying on it here.
inline std::pair<std::uint64_t, std::size_t> read_varuint(std::span<const std::uint8_t> b, std::size_t p) {
    std::uint64_t v = 0;
    int count = 0;
    for (;;) {
        ensure(p < b.size(), "Ion: truncated VarUInt");
        ensure(count < 10, "Ion: VarUInt too long"); // 10*7=70 bits comfortably covers 64
        std::uint8_t byte = b[p++];
        v = (v << 7) | std::uint64_t(byte & 0x7F);
        ++count;
        if (byte & 0x80) break;
    }
    return {v, p};
}

inline void write_varuint(std::vector<std::uint8_t>& out, std::uint64_t v) {
    std::uint8_t tmp[10];
    int n = 0;
    do { tmp[n++] = std::uint8_t(v & 0x7F); v >>= 7; } while (v != 0);
    for (int i = n - 1; i > 0; --i) out.push_back(tmp[i]);       // continuation bytes: high bit clear
    out.push_back(std::uint8_t(tmp[0] | 0x80));                  // last byte: high bit set
}

// A parsed value header. `content_off`/`content_len` bound the value's
// payload uniformly for every type (scalar or container) — Ion's
// length-prefix scheme means skip/value_end never needs a per-type switch
// the way BSON's does.
struct Header {
    std::uint8_t type = kTypeNull;
    std::uint8_t low = 15;
    std::size_t content_off = 0;
    std::size_t content_len = 0;
    bool is_null = true;
    bool bool_value = false;
};

inline Header read_header(std::span<const std::uint8_t> b, std::size_t p) {
    ensure(p < b.size(), "Ion: truncated value header");
    std::uint8_t td = b[p];
    std::uint8_t type = std::uint8_t(td >> 4);
    std::uint8_t low = std::uint8_t(td & 0x0F);
    ensure(type != kTypeReserved, "Ion: reserved type descriptor");
    Header h;
    h.type = type;
    h.low = low;
    h.is_null = (low == 15);
    std::size_t q = p + 1;
    if (h.is_null) {
        h.content_off = q;
        h.content_len = 0;
        return h;
    }
    if (type == kTypeBool) {
        ensure(low == 0 || low == 1, "Ion: invalid bool low nibble");
        h.bool_value = (low == 1);
        h.content_off = q;
        h.content_len = 0;
        return h;
    }
    std::size_t len;
    // Struct's low nibble 1 means "sorted by field SID, VarUInt length
    // follows" — we don't exploit the sort (a linear scan is all a
    // hand-rolled reader needs), but must still parse the length the same
    // way as the low=14 case.
    if (low == 14 || (type == kTypeStruct && low == 1)) {
        auto [vlen, q2] = read_varuint(b, q);
        len = std::size_t(vlen);
        q = q2;
    } else {
        len = low;
    }
    ensure(q + len <= b.size(), "Ion: truncated value content");
    h.content_off = q;
    h.content_len = len;
    return h;
}

inline void write_type_and_length(std::vector<std::uint8_t>& out, std::uint8_t type, std::size_t length) {
    if (length <= 13) {
        out.push_back(std::uint8_t((type << 4) | std::uint8_t(length)));
    } else {
        out.push_back(std::uint8_t((type << 4) | 0x0E));
        write_varuint(out, std::uint64_t(length));
    }
}

// If `p` is an annotation wrapper, returns the position of the wrapped
// value (the header/content of a well-formed Ion stream never nests
// annotation wrappers, so a single unwrap is sufficient). Otherwise
// returns `p` unchanged. Every node's accessors are computed against this
// unwrapped position, so isX()/asX() never need to think about annotations.
inline std::size_t unwrap(std::span<const std::uint8_t> b, std::size_t p) {
    ensure(p < b.size(), "Ion: truncated value");
    if ((b[p] >> 4) != kTypeAnnotation) return p;
    Header h = read_header(b, p);
    auto [alen, after_alen] = read_varuint(b, h.content_off);
    std::size_t wrapped = after_alen + std::size_t(alen);
    ensure(wrapped < h.content_off + h.content_len, "Ion: empty annotation wrapper");
    return wrapped;
}

} // namespace ionb

// ===== Reader ==================================================================

class IonDeserializer {
public:
    // Shared, immutable once built: the backing bytes and the resolved
    // local symbol table. Every subview (returned by value from operator[])
    // holds a shared_ptr to the *same* Context rather than re-deriving
    // anything — the same lifetime pattern beve.hpp uses, for the same
    // reason: a bare pointer back to a document that could move or be
    // destroyed on copy is exactly the class of bug that produced a real
    // heap-buffer-overflow in glaze's own lazy reader (see BEVE.md).
    struct Context {
        std::vector<std::uint8_t> owned;
        std::span<const std::uint8_t> buf;
        std::vector<std::string_view> symtab; // index i => SID (kFirstLocalSid + i)
    };

private:
    std::shared_ptr<const Context> ctx_;
    std::size_t pos_ = 0;   // this node's own (possibly annotation-wrapped) header position
    bool empty_ = false;    // true only for a stream with no top-level value at all (reads as null)

    std::span<const std::uint8_t> buf() const { return ctx_->buf; }

    std::size_t real_pos() const { return ionb::unwrap(buf(), pos_); }

    ionb::Header header() const {
        if (empty_) return ionb::Header{};  // default-constructed Header is_null==true
        return ionb::read_header(buf(), real_pos());
    }

    static std::string_view resolve_symbol(const Context& ctx, std::uint32_t sid) {
        if (sid == 0) return std::string_view{};
        if (sid >= 1 && sid <= 9) return ionb::kSystemSymbolText[sid];
        std::size_t idx = sid - ionb::kFirstLocalSid;
        ensure(idx < ctx.symtab.size(), "Ion: unresolvable symbol ID");
        return ctx.symtab[idx];
    }
    std::string_view resolve_symbol(std::uint32_t sid) const { return resolve_symbol(*ctx_, sid); }

    std::uint64_t decode_magnitude(const ionb::Header& h) const {
        ensure(h.content_len <= 8, "Ion: integer magnitude too large for a 64-bit accessor");
        std::uint64_t v = 0;
        for (std::size_t i = 0; i < h.content_len; ++i) v = (v << 8) | buf()[h.content_off + i];
        return v;
    }

    bool is_int_like() const { auto h = header(); return !h.is_null && (h.type == ionb::kTypePosInt || h.type == ionb::kTypeNegInt); }

    // ---- local symbol table parsing (used only at document-construction time) ----

    static void parse_local_symbol_table(std::span<const std::uint8_t> b, std::size_t struct_pos,
                                          std::vector<std::string_view>& symtab) {
        ionb::Header sh = ionb::read_header(b, struct_pos);
        ensure(!sh.is_null, "Ion: $ion_symbol_table annotation on a null struct");
        std::size_t p = sh.content_off;
        std::size_t end = sh.content_off + sh.content_len;
        bool append = false;
        bool has_symbols = false;
        std::size_t symbols_pos = 0;
        while (p < end) {
            auto [sid, vpos] = ionb::read_varuint(b, p);
            ionb::Header vh = ionb::read_header(b, vpos);
            if (sid == ionb::kSidImports) {
                std::uint8_t vtype = b[vpos] >> 4;
                if (!vh.is_null && vtype == ionb::kTypeSymbol) {
                    auto [imp_sid, _] = ionb::read_varuint(b, vh.content_off);
                    if (imp_sid == ionb::kSidIonSymbolTable) append = true;
                } else if (!vh.is_null && vtype == ionb::kTypeList && vh.content_len > 0) {
                    throw DeserializationError("Ion: shared/imported symbol tables are not supported");
                }
            } else if (sid == ionb::kSidSymbols) {
                has_symbols = true;
                symbols_pos = vpos;
            }
            p = vh.content_off + vh.content_len;
        }
        if (!append) symtab.clear();
        if (has_symbols) {
            ionb::Header lh = ionb::read_header(b, symbols_pos);
            if (!lh.is_null) {
                std::size_t q = lh.content_off, lend = lh.content_off + lh.content_len;
                while (q < lend) {
                    ionb::Header eh = ionb::read_header(b, q);
                    if (!eh.is_null && (b[q] >> 4) == ionb::kTypeString) {
                        symtab.push_back(std::string_view(reinterpret_cast<const char*>(&b[eh.content_off]), eh.content_len));
                    } else {
                        symtab.push_back(std::string_view{});
                    }
                    q = eh.content_off + eh.content_len;
                }
            }
        }
    }

    struct AnnotationInfo { bool has_ion_symbol_table; std::size_t wrapped_pos; };
    static AnnotationInfo parse_annotations(std::span<const std::uint8_t> b, std::size_t pos) {
        ionb::Header h = ionb::read_header(b, pos);
        auto [alen, p] = ionb::read_varuint(b, h.content_off);
        std::size_t annot_end = p + std::size_t(alen);
        ensure(annot_end <= h.content_off + h.content_len, "Ion: truncated annotations");
        bool has_symtab = false;
        while (p < annot_end) {
            auto [sid, p2] = ionb::read_varuint(b, p);
            if (sid == ionb::kSidIonSymbolTable) has_symtab = true;
            p = p2;
        }
        return { has_symtab, annot_end };
    }

    struct ScanResult { std::size_t root_pos = 0; bool found = false; };

    // Walks top-level entries once: applies every local-symbol-table
    // definition it finds (building `symtab` incrementally, exactly as far
    // as any real reader must go before it can resolve the first value's
    // field names), skips NOP padding, and stops at the first entry that's
    // an actual value.
    static ScanResult scan_and_build_symtab(std::span<const std::uint8_t> b, std::size_t start,
                                             std::vector<std::string_view>& symtab) {
        std::size_t p = start;
        while (p < b.size()) {
            std::uint8_t type = std::uint8_t(b[p] >> 4);
            std::uint8_t low = std::uint8_t(b[p] & 0x0F);
            if (type == ionb::kTypeNull && low != 15) {
                // NOP padding.
                ionb::Header h = ionb::read_header(b, p);
                p = h.content_off + h.content_len;
                continue;
            }
            if (type == ionb::kTypeAnnotation) {
                auto info = parse_annotations(b, p);
                if (info.has_ion_symbol_table) {
                    ensure((b[info.wrapped_pos] >> 4) == ionb::kTypeStruct,
                           "Ion: $ion_symbol_table annotation on a non-struct value");
                    parse_local_symbol_table(b, info.wrapped_pos, symtab);
                    ionb::Header outer = ionb::read_header(b, p);
                    p = outer.content_off + outer.content_len;
                    continue;
                }
                return { p, true }; // annotated, but not a symbol table -- this is the root value
            }
            return { p, true };
        }
        return { 0, false };
    }

    void init_from_bytes(std::vector<std::uint8_t> bytes) {
        Context local;
        local.owned = std::move(bytes);
        local.buf = std::span<const std::uint8_t>(local.owned.data(), local.owned.size());
        ensure(local.buf.size() >= 4 &&
               local.buf[0] == ionb::kBinaryVersionMarker[0] && local.buf[1] == ionb::kBinaryVersionMarker[1] &&
               local.buf[2] == ionb::kBinaryVersionMarker[2] && local.buf[3] == ionb::kBinaryVersionMarker[3],
               "Ion: missing binary version marker");
        auto scan = scan_and_build_symtab(local.buf, 4, local.symtab);
        ctx_ = std::make_shared<const Context>(std::move(local));
        if (scan.found) {
            pos_ = scan.root_pos;
        } else {
            empty_ = true;
        }
    }

    // Subview ctor: shares ctx_ with the parent document.
    IonDeserializer(std::shared_ptr<const Context> ctx, std::size_t pos) : ctx_(std::move(ctx)), pos_(pos) {}

public:
    // ---- ctors ----
    IonDeserializer() { init_from_bytes(std::vector<std::uint8_t>(ionb::kBinaryVersionMarker.begin(), ionb::kBinaryVersionMarker.end())); }

    explicit IonDeserializer(std::span<const std::uint8_t> bytes) { init_from_bytes(std::vector<std::uint8_t>(bytes.begin(), bytes.end())); }
    explicit IonDeserializer(const std::vector<std::uint8_t>& buf) { init_from_bytes(buf); }
    explicit IonDeserializer(std::vector<std::uint8_t>&& buf) { init_from_bytes(std::move(buf)); }
    explicit IonDeserializer(const std::uint8_t* data, std::size_t n) { init_from_bytes(std::vector<std::uint8_t>(data, data + n)); }

    // ---- type predicates ----
    bool isNull()   const { return header().is_null; }
    bool isBool()   const { auto h = header(); return !h.is_null && h.type == ionb::kTypeBool; }
    bool isInt()    const {
        if (!is_int_like()) return false;
        auto h = header();
        if (h.content_len > 8) return false;
        if (h.type == ionb::kTypePosInt) return h.content_len < 8 || decode_magnitude(h) <= std::uint64_t(INT64_MAX);
        return h.content_len < 8 || decode_magnitude(h) <= std::uint64_t(INT64_MAX) + 1;
    }
    bool isUInt()   const {
        auto h = header();
        return !h.is_null && h.type == ionb::kTypePosInt && h.content_len <= 8;
    }
    bool isFloat()  const { auto h = header(); return !h.is_null && h.type == ionb::kTypeFloat; }
    bool isString() const { auto h = header(); return !h.is_null && (h.type == ionb::kTypeString || h.type == ionb::kTypeSymbol); }
    bool isBlob()   const { auto h = header(); return !h.is_null && h.type == ionb::kTypeBlob; }
    bool isMap()    const { auto h = header(); return !h.is_null && h.type == ionb::kTypeStruct; }
    bool isArray()  const { auto h = header(); return !h.is_null && (h.type == ionb::kTypeList || h.type == ionb::kTypeSexp); }

    // ---- scalar access ----
    std::int64_t asInt64() const {
        auto h = header();
        ensure(!h.is_null && (h.type == ionb::kTypePosInt || h.type == ionb::kTypeNegInt), "Ion: value is not an integer");
        std::uint64_t mag = decode_magnitude(h);
        if (h.type == ionb::kTypePosInt) {
            ensure(mag <= std::uint64_t(INT64_MAX), "Ion: integer out of int64 range");
            return std::int64_t(mag);
        }
        ensure(mag <= std::uint64_t(INT64_MAX) + 1, "Ion: integer out of int64 range");
        if (mag == std::uint64_t(INT64_MAX) + 1) return INT64_MIN;
        return -std::int64_t(mag);
    }
    std::uint64_t asUInt64() const {
        auto h = header();
        ensure(!h.is_null && h.type == ionb::kTypePosInt, "Ion: value is not a non-negative integer");
        return decode_magnitude(h);
    }
    std::int8_t   asInt8()   const { auto v = asInt64();  ensure(v >= INT8_MIN  && v <= INT8_MAX,  "Ion: int8 out of range");  return std::int8_t(v); }
    std::int16_t  asInt16()  const { auto v = asInt64();  ensure(v >= INT16_MIN && v <= INT16_MAX, "Ion: int16 out of range"); return std::int16_t(v); }
    std::int32_t  asInt32()  const { auto v = asInt64();  ensure(v >= INT32_MIN && v <= INT32_MAX, "Ion: int32 out of range"); return std::int32_t(v); }
    std::uint8_t  asUInt8()  const { auto v = asUInt64(); ensure(v <= UINT8_MAX,  "Ion: uint8 out of range");  return std::uint8_t(v); }
    std::uint16_t asUInt16() const { auto v = asUInt64(); ensure(v <= UINT16_MAX, "Ion: uint16 out of range"); return std::uint16_t(v); }
    std::uint32_t asUInt32() const { auto v = asUInt64(); ensure(v <= UINT32_MAX, "Ion: uint32 out of range"); return std::uint32_t(v); }

    double asDouble() const {
        auto h = header();
        ensure(!h.is_null && h.type == ionb::kTypeFloat, "Ion: value is not a float");
        if (h.content_len == 0) return 0.0;
        ensure(h.content_len == 4 || h.content_len == 8, "Ion: unsupported float width");
        std::uint64_t bits = 0;
        for (std::size_t i = 0; i < h.content_len; ++i) bits = (bits << 8) | buf()[h.content_off + i];
        if (h.content_len == 4) {
            std::uint32_t b32 = std::uint32_t(bits);
            float f; std::memcpy(&f, &b32, 4);
            return double(f);
        }
        double d; std::memcpy(&d, &bits, 8);
        return d;
    }
    float asFloat() const { return static_cast<float>(asDouble()); }

    bool asBool() const {
        auto h = header();
        ensure(!h.is_null && h.type == ionb::kTypeBool, "Ion: value is not a boolean");
        return h.bool_value;
    }

    // Symbols resolve and behave exactly like strings (no distinct "symbol"
    // concept in zerialize's model) -- the same treatment jsoncons gives
    // BSON's deprecated symbol_type.
    std::string_view asStringView() const {
        auto h = header();
        if (!h.is_null && h.type == ionb::kTypeSymbol) {
            auto [sid, _] = ionb::read_varuint(buf(), h.content_off);
            return resolve_symbol(std::uint32_t(sid));
        }
        ensure(!h.is_null && h.type == ionb::kTypeString, "Ion: value is not a string");
        return std::string_view(reinterpret_cast<const char*>(&buf()[h.content_off]), h.content_len);
    }
    std::string asString() const { return std::string(asStringView()); }

    std::span<const std::byte> asBlob() const {
        auto h = header();
        ensure(!h.is_null && h.type == ionb::kTypeBlob, "Ion: value is not a blob");
        return { reinterpret_cast<const std::byte*>(&buf()[h.content_off]), h.content_len };
    }

    // ---- map interface ----
    struct KeysView {
        std::shared_ptr<const Context> ctx;
        std::size_t content_off = 0, content_end = 0;

        struct iterator {
            std::shared_ptr<const Context> ctx;
            std::size_t q = 0, end = 0;

            using iterator_category = std::forward_iterator_tag;
            using iterator_concept  = std::forward_iterator_tag;
            using value_type        = std::string_view;
            using difference_type   = std::ptrdiff_t;
            using reference         = std::string_view;

            iterator() = default;

            reference operator*() const {
                auto [sid, vpos] = ionb::read_varuint(ctx->buf, q);
                (void)vpos;
                return resolve_symbol(*ctx, std::uint32_t(sid));
            }
            iterator& operator++() {
                if (q < end) {
                    auto [sid, vpos] = ionb::read_varuint(ctx->buf, q);
                    (void)sid;
                    ionb::Header vh = ionb::read_header(ctx->buf, vpos);
                    q = vh.content_off + vh.content_len;
                }
                return *this;
            }
            iterator operator++(int) { auto tmp = *this; ++(*this); return tmp; }
            friend bool operator==(const iterator& a, const iterator& b) { return a.q == b.q; }
        };

        iterator begin() const { iterator it; it.ctx = ctx; it.q = content_off; it.end = content_end; return it; }
        iterator end()   const { iterator it; it.ctx = ctx; it.q = content_end; it.end = content_end; return it; }
    };

    KeysView mapKeys() const {
        auto h = header();
        ensure(!h.is_null && h.type == ionb::kTypeStruct, "Ion: value is not a struct");
        return KeysView{ ctx_, h.content_off, h.content_off + h.content_len };
    }

    // Like KeysView, but each dereference also hands back the value at that
    // same position -- avoids the O(n) re-scan-from-start that operator[]
    // does when a caller wants (key, value) for every entry (walking the
    // whole struct via mapKeys()+operator[] is O(n^2); this is O(n)). Defined
    // out-of-line below: Entry holds an IonDeserializer by value, which
    // needs the class to be complete first.
    struct EntriesView;
    EntriesView mapEntries() const;

    bool contains(std::string_view key) const {
        auto h = header();
        if (h.is_null || h.type != ionb::kTypeStruct) return false;
        std::size_t q = h.content_off, end = h.content_off + h.content_len;
        while (q < end) {
            auto [sid, vpos] = ionb::read_varuint(buf(), q);
            if (resolve_symbol(std::uint32_t(sid)) == key) return true;
            ionb::Header vh = ionb::read_header(buf(), vpos);
            q = vh.content_off + vh.content_len;
        }
        return false;
    }

    IonDeserializer operator[](std::string_view key) const {
        auto h = header();
        ensure(!h.is_null && h.type == ionb::kTypeStruct, "Ion: value is not a struct");
        std::size_t q = h.content_off, end = h.content_off + h.content_len;
        while (q < end) {
            auto [sid, vpos] = ionb::read_varuint(buf(), q);
            ionb::Header vh = ionb::read_header(buf(), vpos);
            if (resolve_symbol(std::uint32_t(sid)) == key) return IonDeserializer(ctx_, vpos);
            q = vh.content_off + vh.content_len;
        }
        throw DeserializationError("Ion: key not found: " + std::string(key));
    }

    // ---- array interface ----
    std::size_t arraySize() const {
        auto h = header();
        ensure(!h.is_null && (h.type == ionb::kTypeList || h.type == ionb::kTypeSexp), "Ion: value is not an array");
        std::size_t q = h.content_off, end = h.content_off + h.content_len, n = 0;
        while (q < end) { ionb::Header eh = ionb::read_header(buf(), q); q = eh.content_off + eh.content_len; ++n; }
        return n;
    }

    IonDeserializer operator[](std::size_t idx) const {
        auto h = header();
        ensure(!h.is_null && (h.type == ionb::kTypeList || h.type == ionb::kTypeSexp), "Ion: value is not an array");
        std::size_t q = h.content_off, end = h.content_off + h.content_len, i = 0;
        while (q < end) {
            ionb::Header eh = ionb::read_header(buf(), q);
            if (i == idx) return IonDeserializer(ctx_, q);
            q = eh.content_off + eh.content_len;
            ++i;
        }
        throw DeserializationError("Ion: array index out of range");
    }

    // Sequential single-pass array walk -- avoids the O(n) re-scan-from-
    // start that operator[](idx) does when a caller wants every element
    // (walking the whole array via arraySize()+operator[] is O(n^2); this
    // is O(n)). Defined out-of-line below, alongside EntriesView.
    struct ElementsView;
    ElementsView elements() const;

    // ---- debug ----
    std::string to_string() const { std::ostringstream os; dump(os); return os.str(); }

private:
    void dump(std::ostringstream& os) const {
        if (isNull())   { os << "null"; return; }
        if (isBool())   { os << (asBool() ? "true" : "false"); return; }
        if (isInt())    { os << asInt64(); return; }
        if (isUInt())   { os << asUInt64(); return; }
        if (isFloat())  { os << asDouble(); return; }
        if (isString()) { os << '"' << asStringView() << '"'; return; }
        if (isBlob())   { os << "blob[len=" << asBlob().size() << "]"; return; }
        if (isMap()) {
            os << "{";
            bool first = true;
            for (std::string_view k : mapKeys()) {
                if (!first) os << ",";
                first = false;
                os << '"' << k << "\":";
                (*this)[k].dump(os);
            }
            os << "}";
            return;
        }
        if (isArray()) {
            os << "[";
            std::size_t n = arraySize();
            for (std::size_t i = 0; i < n; ++i) {
                if (i) os << ",";
                (*this)[i].dump(os);
            }
            os << "]";
            return;
        }
        os << "?";
    }
};

struct IonDeserializer::EntriesView {
    std::shared_ptr<const Context> ctx;
    std::size_t content_off = 0, content_end = 0;

    struct Entry { std::string_view key; IonDeserializer value; };

    struct iterator {
        std::shared_ptr<const Context> ctx;
        std::size_t q = 0, end = 0;

        using iterator_category = std::forward_iterator_tag;
        using iterator_concept  = std::forward_iterator_tag;
        using value_type        = Entry;
        using difference_type   = std::ptrdiff_t;
        using reference         = Entry;

        iterator() = default;

        reference operator*() const {
            auto [sid, vpos] = ionb::read_varuint(ctx->buf, q);
            return Entry{ IonDeserializer::resolve_symbol(*ctx, std::uint32_t(sid)), IonDeserializer(ctx, vpos) };
        }
        iterator& operator++() {
            if (q < end) {
                auto [sid, vpos] = ionb::read_varuint(ctx->buf, q);
                (void)sid;
                ionb::Header vh = ionb::read_header(ctx->buf, vpos);
                q = vh.content_off + vh.content_len;
            }
            return *this;
        }
        iterator operator++(int) { auto tmp = *this; ++(*this); return tmp; }
        friend bool operator==(const iterator& a, const iterator& b) { return a.q == b.q; }
    };

    iterator begin() const { iterator it; it.ctx = ctx; it.q = content_off; it.end = content_end; return it; }
    iterator end()   const { iterator it; it.ctx = ctx; it.q = content_end; it.end = content_end; return it; }
};

inline IonDeserializer::EntriesView IonDeserializer::mapEntries() const {
    auto h = header();
    ensure(!h.is_null && h.type == ionb::kTypeStruct, "Ion: value is not a struct");
    return EntriesView{ ctx_, h.content_off, h.content_off + h.content_len };
}

struct IonDeserializer::ElementsView {
    std::shared_ptr<const Context> ctx;
    std::size_t content_off = 0, content_end = 0;

    struct iterator {
        std::shared_ptr<const Context> ctx;
        std::size_t q = 0, end = 0;

        using iterator_category = std::forward_iterator_tag;
        using iterator_concept  = std::forward_iterator_tag;
        using value_type        = IonDeserializer;
        using difference_type   = std::ptrdiff_t;
        using reference         = IonDeserializer;

        iterator() = default;

        reference operator*() const { return IonDeserializer(ctx, q); }
        iterator& operator++() {
            if (q < end) {
                ionb::Header eh = ionb::read_header(ctx->buf, q);
                q = eh.content_off + eh.content_len;
            }
            return *this;
        }
        iterator operator++(int) { auto tmp = *this; ++(*this); return tmp; }
        friend bool operator==(const iterator& a, const iterator& b) { return a.q == b.q; }
    };

    iterator begin() const { iterator it; it.ctx = ctx; it.q = content_off; it.end = content_end; return it; }
    iterator end()   const { iterator it; it.ctx = ctx; it.q = content_end; it.end = content_end; return it; }
};

inline IonDeserializer::ElementsView IonDeserializer::elements() const {
    auto h = header();
    ensure(!h.is_null && (h.type == ionb::kTypeList || h.type == ionb::kTypeSexp), "Ion: value is not an array");
    return ElementsView{ ctx_, h.content_off, h.content_off + h.content_len };
}

// ===== Writer ===================================================================

class IonRootSerializer {
public:
    std::vector<std::uint8_t> content_;
    std::vector<std::string> pending_symbols_;                    // new local symbol text, encounter order
    std::unordered_map<std::string, std::uint32_t> symbol_ids_;   // text -> assigned SID (system or local)
    bool wrote_root_ = false;

    IonRootSerializer() = default;

    ZBuffer finish() {
        if (!wrote_root_) content_.push_back(0x0F); // null.null

        std::vector<std::uint8_t> out;
        out.reserve(content_.size() + pending_symbols_.size() * 8 + 16);
        out.insert(out.end(), ionb::kBinaryVersionMarker.begin(), ionb::kBinaryVersionMarker.end());
        if (!pending_symbols_.empty()) append_local_symbol_table(out, pending_symbols_);
        out.insert(out.end(), content_.begin(), content_.end());
        return ZBuffer(std::move(out));
    }

private:
    static void append_local_symbol_table(std::vector<std::uint8_t>& out, const std::vector<std::string>& syms) {
        std::vector<std::uint8_t> list_content;
        for (const auto& s : syms) {
            ionb::write_type_and_length(list_content, ionb::kTypeString, s.size());
            list_content.insert(list_content.end(), s.begin(), s.end());
        }

        std::vector<std::uint8_t> struct_content;
        ionb::write_varuint(struct_content, ionb::kSidSymbols);
        ionb::write_type_and_length(struct_content, ionb::kTypeList, list_content.size());
        struct_content.insert(struct_content.end(), list_content.begin(), list_content.end());

        std::vector<std::uint8_t> annot_ids;
        ionb::write_varuint(annot_ids, ionb::kSidIonSymbolTable);

        std::vector<std::uint8_t> wrapped_struct;
        ionb::write_type_and_length(wrapped_struct, ionb::kTypeStruct, struct_content.size());
        wrapped_struct.insert(wrapped_struct.end(), struct_content.begin(), struct_content.end());

        std::vector<std::uint8_t> wrapper_content;
        ionb::write_varuint(wrapper_content, annot_ids.size());
        wrapper_content.insert(wrapper_content.end(), annot_ids.begin(), annot_ids.end());
        wrapper_content.insert(wrapper_content.end(), wrapped_struct.begin(), wrapped_struct.end());

        ionb::write_type_and_length(out, ionb::kTypeAnnotation, wrapper_content.size());
        out.insert(out.end(), wrapper_content.begin(), wrapper_content.end());
    }
};

class IonSerializer {
    IonRootSerializer* r_;
    // Container content is built into its own buffer (pushed at
    // begin_array/begin_map, spliced into the parent at end_array/end_map)
    // because Ion's length prefix is a *byte count*, not an element count
    // like BEVE/MsgPack give us up front -- unlike BSON's fixed-width
    // length field, Ion's length encoding is itself variable-width
    // (inline 0-13 vs VarUInt), so an in-place reserve-and-patch trick
    // would need to either guess the right width or rely on padding a
    // VarUInt with non-minimal leading continuation bytes. Building each
    // container's content separately and prepending the correctly-sized
    // header once its length is known avoids that risk entirely.
    std::vector<std::vector<std::uint8_t>> stack_;

    std::vector<std::uint8_t>& out() { return stack_.empty() ? r_->content_ : stack_.back(); }

    std::uint32_t sid_for(std::string_view name) {
        std::string key(name);
        auto it = r_->symbol_ids_.find(key);
        if (it != r_->symbol_ids_.end()) return it->second;
        for (std::uint32_t sid = 1; sid <= 9; ++sid) {
            if (ionb::kSystemSymbolText[sid] == name) {
                r_->symbol_ids_.emplace(std::move(key), sid);
                return sid;
            }
        }
        std::uint32_t sid = ionb::kFirstLocalSid + std::uint32_t(r_->pending_symbols_.size());
        r_->pending_symbols_.push_back(key);
        r_->symbol_ids_.emplace(std::move(key), sid);
        return sid;
    }

    static void write_magnitude(std::vector<std::uint8_t>& o, std::uint8_t type, std::uint64_t mag) {
        std::uint8_t bytes[8];
        int n = 0;
        std::uint64_t t = mag;
        while (t) { bytes[n++] = std::uint8_t(t & 0xFF); t >>= 8; }
        ionb::write_type_and_length(o, type, std::size_t(n));
        for (int i = n - 1; i >= 0; --i) o.push_back(bytes[i]);
    }

    void finish_container(std::uint8_t type) {
        std::vector<std::uint8_t> content = std::move(stack_.back());
        stack_.pop_back();
        auto& o = out();
        ionb::write_type_and_length(o, type, content.size());
        o.insert(o.end(), content.begin(), content.end());
    }

public:
    explicit IonSerializer(IonRootSerializer& rs) : r_(&rs) {}

    void null()          { out().push_back(0x0F); r_->wrote_root_ = true; }
    void boolean(bool v) { out().push_back(v ? 0x11 : 0x10); r_->wrote_root_ = true; }

    void int64(std::int64_t v) {
        auto& o = out();
        if (v == 0) { o.push_back(0x20); r_->wrote_root_ = true; return; }
        std::uint64_t mag = v < 0 ? (v == INT64_MIN ? std::uint64_t(INT64_MAX) + 1 : std::uint64_t(-v)) : std::uint64_t(v);
        write_magnitude(o, v < 0 ? ionb::kTypeNegInt : ionb::kTypePosInt, mag);
        r_->wrote_root_ = true;
    }
    void uint64(std::uint64_t v) {
        auto& o = out();
        if (v == 0) { o.push_back(0x20); r_->wrote_root_ = true; return; }
        write_magnitude(o, ionb::kTypePosInt, v);
        r_->wrote_root_ = true;
    }
    void double_(double v) {
        auto& o = out();
        std::uint64_t bits;
        std::memcpy(&bits, &v, 8);
        ionb::write_type_and_length(o, ionb::kTypeFloat, 8);
        for (int i = 7; i >= 0; --i) o.push_back(std::uint8_t(bits >> (8 * i)));
        r_->wrote_root_ = true;
    }
    void string(std::string_view sv) {
        auto& o = out();
        ionb::write_type_and_length(o, ionb::kTypeString, sv.size());
        o.insert(o.end(), sv.begin(), sv.end());
        r_->wrote_root_ = true;
    }
    void binary(std::span<const std::byte> b) {
        auto& o = out();
        ionb::write_type_and_length(o, ionb::kTypeBlob, b.size());
        auto p = reinterpret_cast<const std::uint8_t*>(b.data());
        o.insert(o.end(), p, p + b.size());
        r_->wrote_root_ = true;
    }
    void key(std::string_view k) {
        ionb::write_varuint(out(), sid_for(k)); // bare VarUInt, no type tag (same shape as BSON's headerless keys)
    }

    void begin_array(std::size_t) { stack_.emplace_back(); r_->wrote_root_ = true; }
    void end_array()              { finish_container(ionb::kTypeList); }
    void begin_map(std::size_t)   { stack_.emplace_back(); r_->wrote_root_ = true; }
    void end_map()                { finish_container(ionb::kTypeStruct); }
};

struct Ion {
    static inline constexpr const char* Name = "Ion";
    using Deserializer   = IonDeserializer;
    using RootSerializer = IonRootSerializer;
    using Serializer     = IonSerializer;
};

} // namespace zerialize
