// BSON protocol implemented with jsoncons (writer) + a hand-rolled reader.
//
// The writer wraps jsoncons::bson::bson_bytes_encoder, a SAX-style visitor
// with the same shape as jsoncons::cbor::cbor_bytes_encoder (already used
// by protocols/cbor.hpp) -- begin_array/begin_object/key/int64_value/etc
// map almost one-to-one onto zerialize's Writer concept.
//
// The reader is hand-rolled and does not use jsoncons at all, matching the
// precedent already set by cbor.hpp: jsoncons is linked in for its encoder,
// but CborDeserializer is an independent span-walking parser, not a wrapper
// around jsoncons' own DOM/cursor readers. Same reasoning applies here --
// zero-copy laziness and full control over the Reader concept's
// random-access semantics. BSON's format makes this easier than CBOR: no
// indefinite-length forms, no variable-width integers, every length is a
// plain little-endian int32/int64.
//
// See BSON.md for the wire format, the two format-inherent constraints
// (no top-level scalar, no unsigned 64-bit wire type), and known
// limitations (MongoDB-specific extension types are skippable but not
// readable as scalars).
#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include <span>
#include <stdexcept>
#include <sstream>
#include <limits>
#include <type_traits>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons_ext/bson/bson_type.hpp>

#include <zerialize/zbuffer.hpp>
#include <zerialize/errors.hpp>

namespace zerialize {
namespace bsonjc {

// ========================== Writer (Serializer) ===============================

struct RootSerializer {
    std::vector<uint8_t> out_;
    jsoncons::bson::bson_bytes_encoder enc;
    bool wrote_root = false;

    RootSerializer()
        : out_()
        , enc(out_)
    {}

    ZBuffer finish() {
        if (!wrote_root) {
            // BSON has no top-level scalar or null -- the only well-formed
            // "nothing was ever written" buffer is an empty document.
            try {
                enc.begin_object();
                enc.end_object();
            } catch (const std::exception& e) {
                throw SerializationError(std::string("BSON: ") + e.what());
            }
            wrote_root = true;
        }
        return ZBuffer(std::move(out_));
    }
};

struct Serializer {
    RootSerializer* r;
    explicit Serializer(RootSerializer& rs) : r(&rs) {}

    // jsoncons' encoder throws jsoncons::ser_error (a std::exception) on
    // format-inherent failures we want to surface as our own error type
    // rather than let jsoncons' exception leak through: a bare top-level
    // scalar (BSON requires the root to be a document or array) and a
    // uint64() value above INT64_MAX (BSON has no unsigned 64-bit wire
    // type -- see BSON.md).
    template <class F>
    static void guarded(F&& f) {
        try { f(); }
        catch (const std::exception& e) { throw SerializationError(std::string("BSON: ") + e.what()); }
    }

    void null()                  { guarded([&]{ r->enc.null_value(); }); r->wrote_root = true; }
    void boolean(bool v)         { guarded([&]{ r->enc.bool_value(v); }); r->wrote_root = true; }
    void int64(std::int64_t v)   { guarded([&]{ r->enc.int64_value(v); }); r->wrote_root = true; }
    void uint64(std::uint64_t v) { guarded([&]{ r->enc.uint64_value(v); }); r->wrote_root = true; }
    void double_(double v)       { guarded([&]{ r->enc.double_value(v); }); r->wrote_root = true; }
    void string(std::string_view sv) { guarded([&]{ r->enc.string_value(sv); }); r->wrote_root = true; }
    void binary(std::span<const std::byte> b) {
        guarded([&]{
            const uint8_t* p = reinterpret_cast<const uint8_t*>(b.data());
            std::vector<uint8_t> tmp(p, p + b.size());
            // Explicit raw_tag=0x00 ("generic binary" subtype, the BSON
            // spec's conventional choice) -- the tag-less overload defaults
            // to 0x80 ("user-defined"), which round-trips fine through our
            // own reader but is a less interoperable choice for bytes that
            // might be read by other BSON tooling.
            r->enc.byte_string_value(tmp, std::uint64_t(0x00));
        });
        r->wrote_root = true;
    }

    void begin_array(std::size_t n) { guarded([&]{ r->enc.begin_array(n); }); r->wrote_root = true; }
    void end_array()                { guarded([&]{ r->enc.end_array(); }); }
    void begin_map(std::size_t n)   { guarded([&]{ r->enc.begin_object(n); }); r->wrote_root = true; }
    void end_map()                  { guarded([&]{ r->enc.end_object(); }); }
    void key(std::string_view k)    { guarded([&]{ r->enc.key(k); }); }
};

// ========================== Reader (Deserializer) =============================

// Unlike CBOR/MsgPack/BEVE, a raw span of BSON *value* bytes doesn't self-
// identify its type (e.g. 4 bytes could be an int32, or the start of a
// string's length prefix, or part of something else entirely) -- the type
// tag lives once in the parent element's header, not repeated in the value.
// So every node here explicitly carries the type byte it was read with,
// rather than recovering it by peeking bytes the way the other hand-rolled
// readers do.
class BsonDeserializer {
    std::span<const uint8_t> buf_{}; // whole backing buffer (root document)
    std::vector<uint8_t> owned_;     // if non-empty, buf_ points into this
    std::size_t pos_ = 0;            // offset where this node's *value* bytes begin
    std::uint8_t type_ = jsoncons::bson::bson_type::document_type;

    static void ensure(bool cond, const char* msg) {
        if (!cond) throw DeserializationError(msg);
    }

    static std::uint32_t read_u32(std::span<const uint8_t> b, std::size_t p) {
        ensure(p + 4 <= b.size(), "BSON: truncated int32");
        return std::uint32_t(b[p]) | (std::uint32_t(b[p+1])<<8) | (std::uint32_t(b[p+2])<<16) | (std::uint32_t(b[p+3])<<24);
    }
    static std::uint64_t read_u64(std::span<const uint8_t> b, std::size_t p) {
        ensure(p + 8 <= b.size(), "BSON: truncated int64/double");
        std::uint64_t v = 0;
        for (int i = 7; i >= 0; --i) v = (v << 8) | b[p + std::size_t(i)];
        return v;
    }

    // Offset one past this value's bytes, given where it starts and its type.
    // Every branch validates the bytes it claims exist in `b` before
    // trusting them, so a truncated/corrupt value can't make a caller read
    // (or hand out a span) past the end of the buffer.
    static std::size_t value_end(std::span<const uint8_t> b, std::size_t v, std::uint8_t t) {
        using namespace jsoncons::bson::bson_type;
        switch (t) {
            case double_type: case int64_type: case datetime_type: case timestamp_type:
                ensure(v + 8 <= b.size(), "BSON: truncated 8-byte value");
                return v + 8;
            case int32_type:
                ensure(v + 4 <= b.size(), "BSON: truncated int32");
                return v + 4;
            case bool_type:
                ensure(v + 1 <= b.size(), "BSON: truncated bool");
                return v + 1;
            case null_type: case undefined_type: case min_key_type: case max_key_type:
                return v;
            case object_id_type:
                ensure(v + 12 <= b.size(), "BSON: truncated object id");
                return v + 12;
            case decimal128_type:
                ensure(v + 16 <= b.size(), "BSON: truncated decimal128");
                return v + 16;
            case string_type: case symbol_type: case javascript_type: {
                std::uint32_t len = read_u32(b, v); // includes the trailing NUL
                ensure(len >= 1, "BSON: invalid string length");
                std::size_t end = v + 4 + std::size_t(len);
                ensure(end <= b.size(), "BSON: truncated string body");
                return end;
            }
            case document_type: case array_type: case javascript_with_scope_type: {
                std::uint32_t len = read_u32(b, v); // self-inclusive total length
                ensure(len >= 5, "BSON: invalid document/array length");
                std::size_t end = v + std::size_t(len);
                ensure(end <= b.size(), "BSON: truncated document/array body");
                return end;
            }
            case binary_type: {
                std::uint32_t len = read_u32(b, v); // payload length, excludes subtype byte
                std::size_t end = v + 4 + 1 + std::size_t(len);
                ensure(end <= b.size(), "BSON: truncated binary body");
                return end;
            }
            case regex_type: {
                std::size_t q = v;
                for (int part = 0; part < 2; ++part) {
                    for (;;) {
                        ensure(q < b.size(), "BSON: truncated regex");
                        if (b[q] == 0x00) { ++q; break; }
                        ++q;
                    }
                }
                return q;
            }
            default:
                throw DeserializationError("BSON: unknown or unsupported element type");
        }
    }

    struct Elem { std::uint8_t type; std::string_view name; std::size_t value_off; std::size_t end_off; };

    // Parses one element (type byte + NUL-terminated name + value) starting
    // at `p`, which must point at the type byte.
    static Elem read_elem(std::span<const uint8_t> b, std::size_t p) {
        ensure(p < b.size(), "BSON: truncated element");
        std::uint8_t t = b[p];
        std::size_t name_start = p + 1;
        std::size_t nul = name_start;
        while (true) {
            ensure(nul < b.size(), "BSON: unterminated element name");
            if (b[nul] == 0x00) break;
            ++nul;
        }
        std::string_view name(reinterpret_cast<const char*>(&b[name_start]), nul - name_start);
        std::size_t value_off = nul + 1;
        std::size_t end_off = value_end(b, value_off, t);
        return { t, name, value_off, end_off };
    }

    // [first-element-position, terminator-position) for a document/array
    // node whose value starts at `v`. Bounds are validated once here (via
    // value_end), so callers can walk `[first, term)` without re-checking
    // against buf_.size() themselves, and a document missing its 0x00
    // terminator within its declared length can't make a walk wander into
    // a sibling element's bytes.
    struct ContainerSpan { std::size_t first; std::size_t term; };
    static ContainerSpan container_span(std::span<const uint8_t> b, std::size_t v) {
        std::size_t end = value_end(b, v, jsoncons::bson::bson_type::document_type); // same layout as array_type
        return { v + 4, end - 1 };
    }

    std::int64_t rawInt64() const {
        using namespace jsoncons::bson::bson_type;
        if (type_ == int32_type) { ensure(pos_ + 4 <= buf_.size(), "BSON: truncated int32"); return std::int64_t(std::int32_t(read_u32(buf_, pos_))); }
        if (type_ == int64_type) { return std::int64_t(read_u64(buf_, pos_)); }
        throw DeserializationError("BSON: value is not an integer");
    }

    // subview ctor: shares buf_ with the parent (caller must keep whatever
    // owns buf_'s storage alive, same convention as msgpack.hpp/cbor.hpp).
    BsonDeserializer(std::span<const uint8_t> buf, std::size_t value_off, std::uint8_t type)
        : buf_(buf), pos_(value_off), type_(type) {}

public:
    // ---- ctors ----
    BsonDeserializer() = default;

    explicit BsonDeserializer(std::span<const uint8_t> bytes) : buf_(bytes), pos_(0) {}
    explicit BsonDeserializer(const std::vector<uint8_t>& buf)
        : owned_(buf.begin(), buf.end())
    { buf_ = std::span<const uint8_t>(owned_.data(), owned_.size()); }
    explicit BsonDeserializer(std::vector<uint8_t>&& buf)
        : owned_(std::move(buf))
    { buf_ = std::span<const uint8_t>(owned_.data(), owned_.size()); }
    explicit BsonDeserializer(const uint8_t* data, std::size_t n) : BsonDeserializer(std::span<const uint8_t>(data, n)) {}

    // ---- type predicates ----
    bool isNull()   const { using namespace jsoncons::bson::bson_type; return type_ == null_type || type_ == undefined_type; }
    bool isBool()   const { return type_ == jsoncons::bson::bson_type::bool_type; }
    bool isInt()    const { using namespace jsoncons::bson::bson_type; return type_ == int32_type || type_ == int64_type; }
    bool isUInt()   const { return isInt() && rawInt64() >= 0; } // BSON has no unsigned wire type; see BSON.md
    bool isFloat()  const { return type_ == jsoncons::bson::bson_type::double_type; }
    bool isString() const { using namespace jsoncons::bson::bson_type; return type_ == string_type || type_ == symbol_type; }
    bool isBlob()   const { return type_ == jsoncons::bson::bson_type::binary_type; }
    bool isMap()    const { return type_ == jsoncons::bson::bson_type::document_type; }
    bool isArray()  const { return type_ == jsoncons::bson::bson_type::array_type; }

    // ---- scalar access ----
    std::int8_t   asInt8()   const { auto v = asInt64();  if (v < INT8_MIN  || v > INT8_MAX)  throw DeserializationError("BSON: int8 out of range");  return std::int8_t(v); }
    std::int16_t  asInt16()  const { auto v = asInt64();  if (v < INT16_MIN || v > INT16_MAX) throw DeserializationError("BSON: int16 out of range"); return std::int16_t(v); }
    std::int32_t  asInt32()  const { auto v = asInt64();  if (v < INT32_MIN || v > INT32_MAX) throw DeserializationError("BSON: int32 out of range"); return std::int32_t(v); }
    std::int64_t  asInt64()  const { return rawInt64(); }

    std::uint8_t  asUInt8()  const { auto v = asUInt64(); if (v > UINT8_MAX)  throw DeserializationError("BSON: uint8 out of range");  return std::uint8_t(v); }
    std::uint16_t asUInt16() const { auto v = asUInt64(); if (v > UINT16_MAX) throw DeserializationError("BSON: uint16 out of range"); return std::uint16_t(v); }
    std::uint32_t asUInt32() const { auto v = asUInt64(); if (v > UINT32_MAX) throw DeserializationError("BSON: uint32 out of range"); return std::uint32_t(v); }
    std::uint64_t asUInt64() const {
        auto v = rawInt64();
        if (v < 0) throw DeserializationError("BSON: negative value has no uint64 representation");
        return std::uint64_t(v);
    }

    float  asFloat()  const { return static_cast<float>(asDouble()); }
    double asDouble() const {
        ensure(isFloat(), "BSON: value is not a double");
        std::uint64_t bits = read_u64(buf_, pos_);
        double d; std::memcpy(&d, &bits, 8);
        return d;
    }
    bool asBool() const {
        ensure(isBool(), "BSON: value is not a boolean");
        ensure(pos_ + 1 <= buf_.size(), "BSON: truncated bool");
        return buf_[pos_] != 0;
    }

    std::string asString() const { auto sv = asStringView(); return std::string(sv); }
    std::string_view asStringView() const {
        ensure(isString(), "BSON: value is not a string");
        std::uint32_t len = read_u32(buf_, pos_); // includes trailing NUL
        ensure(len >= 1, "BSON: invalid string length");
        std::size_t n = std::size_t(len) - 1;
        ensure(pos_ + 4 + n <= buf_.size(), "BSON: truncated string body");
        return std::string_view(reinterpret_cast<const char*>(&buf_[pos_ + 4]), n);
    }

    std::span<const std::byte> asBlob() const {
        ensure(isBlob(), "BSON: value is not binary");
        std::uint32_t len = read_u32(buf_, pos_);
        std::size_t start = pos_ + 4 + 1; // length field + subtype byte
        ensure(start + std::size_t(len) <= buf_.size(), "BSON: truncated binary body");
        return { reinterpret_cast<const std::byte*>(&buf_[start]), std::size_t(len) };
    }

    // ---- map interface ----
    struct KeysView {
        std::span<const uint8_t> buf;
        std::size_t value_off; // the map node's own value offset

        struct iterator {
            std::span<const uint8_t> buf{};
            std::size_t q = 0;

            using iterator_category = std::forward_iterator_tag;
            using iterator_concept  = std::forward_iterator_tag;
            using value_type        = std::string_view;
            using difference_type   = std::ptrdiff_t;
            using reference         = std::string_view;

            iterator() = default;

            reference operator*() const { return read_elem(buf, q).name; }
            iterator& operator++() {
                if (q < buf.size() && buf[q] != 0x00) q = read_elem(buf, q).end_off;
                return *this;
            }
            iterator operator++(int) { auto tmp = *this; ++(*this); return tmp; }
            friend bool operator==(const iterator& a, const iterator& b) { return a.q == b.q && a.buf.data() == b.buf.data(); }
        };

        iterator begin() const { auto cs = container_span(buf, value_off); iterator it; it.buf = buf; it.q = cs.first; return it; }
        iterator end()   const { auto cs = container_span(buf, value_off); iterator it; it.buf = buf; it.q = cs.term;  return it; }
    };

    KeysView mapKeys() const {
        ensure(isMap(), "BSON: value is not a document");
        return KeysView{ buf_, pos_ };
    }

    bool contains(std::string_view key) const {
        if (!isMap()) return false;
        auto cs = container_span(buf_, pos_);
        for (std::size_t q = cs.first; q < cs.term; ) {
            auto e = read_elem(buf_, q);
            if (e.name == key) return true;
            q = e.end_off;
        }
        return false;
    }

    BsonDeserializer operator[](std::string_view key) const {
        ensure(isMap(), "BSON: value is not a document");
        auto cs = container_span(buf_, pos_);
        for (std::size_t q = cs.first; q < cs.term; ) {
            auto e = read_elem(buf_, q);
            if (e.name == key) return BsonDeserializer(buf_, e.value_off, e.type);
            q = e.end_off;
        }
        throw DeserializationError("BSON: key not found: " + std::string(key));
    }

    // ---- array interface ----
    std::size_t arraySize() const {
        ensure(isArray(), "BSON: value is not an array");
        auto cs = container_span(buf_, pos_);
        std::size_t n = 0;
        for (std::size_t q = cs.first; q < cs.term; ++n) q = read_elem(buf_, q).end_off;
        return n;
    }

    BsonDeserializer operator[](std::size_t idx) const {
        ensure(isArray(), "BSON: value is not an array");
        auto cs = container_span(buf_, pos_);
        std::size_t i = 0;
        for (std::size_t q = cs.first; q < cs.term; ++i) {
            auto e = read_elem(buf_, q);
            if (i == idx) return BsonDeserializer(buf_, e.value_off, e.type);
            q = e.end_off;
        }
        throw DeserializationError("BSON: array index out of range");
    }

    // ---- debug ----
    std::string to_string() const {
        std::ostringstream os;
        dump(os);
        return os.str();
    }

private:
    void dump(std::ostringstream& os) const {
        if (isNull())        { os << "null"; return; }
        if (isBool())        { os << (asBool() ? "true" : "false"); return; }
        if (isInt())         { os << asInt64(); return; }
        if (isFloat())       { os << asDouble(); return; }
        if (isString())      { os << '"' << asStringView() << '"'; return; }
        if (isBlob())        { os << "blob[len=" << asBlob().size() << "]"; return; }
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

} // namespace bsonjc

struct Bson {
    static inline constexpr const char* Name = "Bson";
    using Deserializer   = bsonjc::BsonDeserializer;
    using RootSerializer = bsonjc::RootSerializer;
    using Serializer     = bsonjc::Serializer;
};

} // namespace zerialize
