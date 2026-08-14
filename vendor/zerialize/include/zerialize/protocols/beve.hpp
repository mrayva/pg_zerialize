#pragma once
//
// BEVE protocol adapter.
//
// The *reader* wraps glaze's lazy BEVE view (glz::lazy_beve_document /
// glz::lazy_beve_view, see <glaze/beve/lazy.hpp>): a non-owning,
// bounds-checked, zero-copy walk over raw BEVE bytes. We adapt its API to
// zerialize's Reader concept rather than re-deriving BEVE's tag-byte
// parsing by hand.
//
// The *writer* is a small hand-rolled byte-appender with no glaze
// dependency: BEVE containers are length-prefixed with the count already
// known up front (zerialize's begin_array(n)/begin_map(n) already supply
// it), so a single streaming pass is all that's needed -- the same shape
// as protocols/msgpack.hpp's writer. glaze's own write path is
// reflection/trait-driven (glz::write_beve<T>) and has no equivalent
// low-level streaming entry point; building a glz::generic tree from our
// Writer calls and serializing it once would also silently change the
// wire encoding (glaze never picks BEVE's "typed array" optimization for
// a heterogeneous glz::generic tree, only for statically-typed
// containers), which would break round-tripping through our own
// isBlob()/asBlob(). Hand-writing the bytes directly avoids that
// mismatch and keeps the encoding under our control.
//
// See BEVE.md for the wire format and known v1 limitations.

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <glaze/beve/lazy.hpp>

#include <zerialize/zbuffer.hpp>
#include <zerialize/errors.hpp>

namespace zerialize {

// ===== Deserializer (adapts glz::lazy_beve_document / lazy_beve_view) ========
class BeveDeserializer {
public:
    static constexpr auto kOpts = glz::opts{};
    using Doc  = glz::lazy_beve_document<kOpts>;
    using View = glz::lazy_beve_view<kOpts>;
    using Iter = glz::lazy_beve_iterator<kOpts>;

private:
    // A node pulled out of a *typed* array (operator[](idx) on a typed-array
    // parent) has no tag byte of its own -- that's the point of "typed": the
    // element kind lives once on the array's header, not repeated per
    // element. glz's own is_number()/is_string()/etc. peek the node's first
    // byte directly and are only meaningful for self-tagged nodes, so such
    // elements would be misclassified by peeking. When synth_ != None, our
    // isX() predicates below trust it instead of touching view_'s bytes.
    enum class SynthKind { None, Float, Signed, Unsigned, StringElem };

    // Keeps the raw bytes alive; glz::lazy_beve_view stores pointers
    // directly into this buffer.
    std::shared_ptr<const std::vector<uint8_t>> owned_bytes_;
    // glz::lazy_beve_view also stores a raw pointer *back* to its parent
    // glz::lazy_beve_document, so that document must never move or be
    // destroyed while any view referencing it is alive. A shared_ptr gives
    // every subview (returned by value from operator[]) a stable heap
    // address to point at, kept alive by refcount regardless of how many
    // BeveDeserializer copies exist.
    std::shared_ptr<Doc> doc_;
    View view_{};
    SynthKind synth_ = SynthKind::None;

    struct ElemInfo {
        bool numeric = false;
        bool isString = false;
        bool isBool = false;
        std::uint8_t cat = 0;      // 0 float, 1 signed, 2 unsigned (numeric only)
        std::uint8_t widthIdx = 0; // 0..3 -> 1,2,4,8 bytes (numeric only)
    };

    // Peek *this* node's own header to learn what kind its typed-array
    // elements are. Only meaningful when view_.is_typed_array() and this
    // node itself is self-tagged (synth_ == None).
    ElemInfo elementInfo() const {
        ElemInfo info;
        const std::uint8_t t = std::uint8_t(*view_.data());
        const std::uint8_t elemType = (t >> 3) & 0b11;
        if (elemType < 3) {
            info.numeric = true;
            info.cat = elemType;
            info.widthIdx = t >> 5;
        } else if ((t >> 5) & 1) {
            info.isString = true;
        } else {
            info.isBool = true;
        }
        return info;
    }

    static SynthKind synthKindFor(const ElemInfo& info) {
        if (info.numeric) {
            switch (info.cat) {
                case 0: return SynthKind::Float;
                case 1: return SynthKind::Signed;
                default: return SynthKind::Unsigned;
            }
        }
        if (info.isString) return SynthKind::StringElem;
        // Packed boolean typed arrays aren't randomly indexable (glaze's own
        // lazy operator[] refuses them too -- see BEVE.md); operator[](idx)
        // below throws before this would matter.
        return SynthKind::None;
    }

    bool isNumberNode() const {
        return synth_ == SynthKind::Float || synth_ == SynthKind::Signed || synth_ == SynthKind::Unsigned ||
               (synth_ == SynthKind::None && view_.is_number());
    }

    SynthKind numberKind() const {
        if (synth_ != SynthKind::None) return synth_;
        const std::uint8_t t = std::uint8_t(*view_.data());
        const std::uint8_t cat = (t >> 3) & 0b11;
        return cat == 0 ? SynthKind::Float : (cat == 1 ? SynthKind::Signed : SynthKind::Unsigned);
    }

    // Validates a view returned by indexing/lookup before trusting it.
    // glz::lazy_beve_view::operator[](size_t) on a *generic* array does not
    // itself verify that the resulting element's start position is within
    // the buffer when the array's declared element count exceeds what's
    // actually present -- has_error() stays false, and the returned view's
    // data() can point at-or-past the end of the buffer (confirmed against
    // glaze v8.0.0 with ASan: dereferencing such a view is a heap-buffer-
    // overflow). Since BEVE input may be untrusted, close that gap here
    // rather than trusting has_error() alone. This only checks that the
    // element *starts* in bounds; get<T>() below independently bounds-checks
    // that a scalar's full declared width fits before the buffer end.
    static void checkChild(const View& v, const char* what) {
        if (v.has_error()) throw DeserializationError(std::string("beve: ") + what);
        if (!v.data() || v.data() >= v.beve_end()) {
            throw DeserializationError(std::string("beve: ") + what + " (corrupt or truncated data)");
        }
    }

    template <class T, class From>
    static T rangeCast(From v) {
        if (v < static_cast<From>(std::numeric_limits<T>::min()) ||
            v > static_cast<From>(std::numeric_limits<T>::max())) {
            throw DeserializationError("beve: value out of range for target type");
        }
        return static_cast<T>(v);
    }

    void initFromBytes(std::vector<uint8_t> bytes) {
        auto owned = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
        auto parsed = glz::lazy_beve<kOpts>(std::span<const uint8_t>(*owned));
        if (!parsed) throw DeserializationError("beve: failed to parse document");
        owned_bytes_ = std::move(owned);
        doc_ = std::make_shared<Doc>(std::move(*parsed));
        view_ = doc_->root();
    }

    // Subview ctor: shares doc_/owned_bytes_ with the parent document.
    BeveDeserializer(std::shared_ptr<const std::vector<uint8_t>> owned,
                      std::shared_ptr<Doc> doc,
                      View v,
                      SynthKind synth)
        : owned_bytes_(std::move(owned)), doc_(std::move(doc)), view_(v), synth_(synth) {}

public:
    // ---- ctors ----
    BeveDeserializer() { initFromBytes({0x00}); } // empty document = null

    explicit BeveDeserializer(std::span<const uint8_t> bytes) {
        initFromBytes(std::vector<uint8_t>(bytes.begin(), bytes.end()));
    }
    explicit BeveDeserializer(const uint8_t* data, std::size_t n) {
        initFromBytes(std::vector<uint8_t>(data, data + n));
    }
    explicit BeveDeserializer(const std::vector<uint8_t>& buf) { initFromBytes(buf); }
    explicit BeveDeserializer(std::vector<uint8_t>&& buf) { initFromBytes(std::move(buf)); }

    // ---- type predicates ----
    bool isNull()  const { return synth_ == SynthKind::None && view_.is_null(); }
    bool isBool()  const { return synth_ == SynthKind::None && view_.is_boolean(); }
    bool isInt()   const { return isNumberNode() && numberKind() == SynthKind::Signed; }
    bool isUInt()  const { return isNumberNode() && numberKind() == SynthKind::Unsigned; }
    bool isFloat() const { return isNumberNode() && numberKind() == SynthKind::Float; }
    bool isString()const { return synth_ == SynthKind::StringElem || (synth_ == SynthKind::None && view_.is_string()); }
    bool isMap()   const { return synth_ == SynthKind::None && view_.is_object(); }

    bool isBlob() const {
        if (synth_ != SynthKind::None || !view_.is_typed_array()) return false;
        auto info = elementInfo();
        return info.numeric && info.cat == 2 /* unsigned */ && info.widthIdx == 0 /* 1 byte */;
    }
    bool isArray() const { return synth_ == SynthKind::None && view_.is_array() && !isBlob(); }

    // ---- scalar access ----
    std::int64_t asInt64() const {
        auto r = view_.template get<std::int64_t>();
        if (!r) throw DeserializationError("beve: value is not an integer");
        return *r;
    }
    std::uint64_t asUInt64() const {
        auto r = view_.template get<std::uint64_t>();
        if (!r) throw DeserializationError("beve: value is not an integer");
        return *r;
    }
    double asDouble() const {
        auto r = view_.template get<double>();
        if (!r) throw DeserializationError("beve: value is not a number");
        return *r;
    }
    float asFloat() const { return static_cast<float>(asDouble()); }

    std::int8_t   asInt8()   const { return rangeCast<std::int8_t>(asInt64()); }
    std::int16_t  asInt16()  const { return rangeCast<std::int16_t>(asInt64()); }
    std::int32_t  asInt32()  const { return rangeCast<std::int32_t>(asInt64()); }
    std::uint8_t  asUInt8()  const { return rangeCast<std::uint8_t>(asUInt64()); }
    std::uint16_t asUInt16() const { return rangeCast<std::uint16_t>(asUInt64()); }
    std::uint32_t asUInt32() const { return rangeCast<std::uint32_t>(asUInt64()); }

    bool asBool() const {
        auto r = view_.template get<bool>();
        if (!r) throw DeserializationError("beve: value is not a boolean");
        return *r;
    }

    std::string asString() const {
        auto r = view_.template get<std::string>();
        if (!r) throw DeserializationError("beve: value is not a string");
        return *r;
    }
    std::string_view asStringView() const {
        auto r = view_.template get<std::string_view>();
        if (!r) throw DeserializationError("beve: value is not a string");
        return *r;
    }

    // Blobs are written (and, for round-tripping, expected) as a BEVE typed
    // array of uint8 -- a single unaligned byte run right after the header
    // + compressed size, so element 0's pointer gives us a zero-copy span
    // over the whole payload.
    std::span<const std::byte> asBlob() const {
        if (!isBlob()) throw DeserializationError("beve: value is not a blob");
        const std::size_t n = view_.size(); // declared count -- not yet trusted
        if (n == 0) return {};
        View e0 = view_[0];
        checkChild(e0, "corrupt typed byte array");
        // e0's per-element bounds check (in checkChild) only guarantees byte 0
        // exists; the declared count n is otherwise unverified attacker-
        // controlled input, and reading it as a flat n-byte span bypasses
        // get<T>()'s own per-element bounds checking. Clamp against what's
        // actually left in the buffer before handing out the span.
        const std::size_t available = static_cast<std::size_t>(view_.beve_end() - e0.data());
        if (n > available) throw DeserializationError("beve: truncated typed byte array");
        return { reinterpret_cast<const std::byte*>(e0.data()), n };
    }

    // ---- map interface ----
    struct KeysView {
        View v;

        struct iterator {
            Iter it{};

            using iterator_category = std::forward_iterator_tag;
            using iterator_concept  = std::forward_iterator_tag;
            using value_type        = std::string_view;
            using difference_type   = std::ptrdiff_t;
            using reference         = std::string_view;

            iterator() = default;
            explicit iterator(Iter i) : it(std::move(i)) {}

            reference operator*() const { return (*it).key(); }
            iterator& operator++() { ++it; return *this; }
            iterator operator++(int) { auto tmp = *this; ++(*this); return tmp; }
            friend bool operator==(const iterator& a, const iterator& b) { return a.it == b.it; }
        };

        iterator begin() const { return iterator{v.begin()}; }
        iterator end()   const { return iterator{v.end()}; }
    };

    KeysView mapKeys() const {
        if (!isMap()) throw DeserializationError("beve: value is not a map");
        return KeysView{view_};
    }

    bool contains(std::string_view key) const {
        return isMap() && view_.contains(key);
    }

    BeveDeserializer operator[](std::string_view key) const {
        if (!isMap()) throw DeserializationError("beve: value is not a map");
        View child = view_[key];
        checkChild(child, ("key not found: " + std::string(key)).c_str());
        return BeveDeserializer(owned_bytes_, doc_, child, SynthKind::None);
    }

    // ---- array interface ----
    std::size_t arraySize() const {
        if (synth_ != SynthKind::None || !view_.is_array()) throw DeserializationError("beve: value is not an array");
        return view_.size();
    }

    BeveDeserializer operator[](std::size_t idx) const {
        if (synth_ != SynthKind::None || !view_.is_array()) throw DeserializationError("beve: value is not an array");
        SynthKind childKind = SynthKind::None;
        if (view_.is_typed_array()) {
            childKind = synthKindFor(elementInfo());
        }
        View child = view_[idx];
        checkChild(child, "array index out of range");
        return BeveDeserializer(owned_bytes_, doc_, child, childKind);
    }

    // ---- debug ----
    std::string to_string() const {
        if (isNull())   return "null";
        if (isBool())   return asBool() ? "true" : "false";
        if (isInt())    return std::to_string(asInt64());
        if (isUInt())   return std::to_string(asUInt64());
        if (isFloat())  return std::to_string(asDouble());
        if (isString()) return std::string("\"") + std::string(asStringView()) + "\"";
        if (isBlob())   return "blob[len=" + std::to_string(asBlob().size()) + "]";
        if (isMap()) {
            std::ostringstream os;
            os << "{";
            bool first = true;
            for (std::string_view k : mapKeys()) {
                if (!first) os << ",";
                first = false;
                os << "\"" << k << "\":" << (*this)[k].to_string();
            }
            os << "}";
            return os.str();
        }
        if (isArray()) {
            std::ostringstream os;
            os << "[";
            const std::size_t n = arraySize();
            for (std::size_t i = 0; i < n; ++i) {
                if (i) os << ",";
                os << (*this)[i].to_string();
            }
            os << "]";
            return os.str();
        }
        return "?";
    }
};

// ===== Serializer (hand-rolled BEVE byte writer, no glaze dependency) ========
namespace beve_detail {

// Wire type field (bits 0-2 of a header byte).
inline constexpr std::uint8_t kTypeNumber      = 0b001;
inline constexpr std::uint8_t kTypeString      = 0b010;
inline constexpr std::uint8_t kTypeObject      = 0b011;
inline constexpr std::uint8_t kTypeTypedArray  = 0b100;
inline constexpr std::uint8_t kTypeGenericArray= 0b101;

// Number/typed-array category field (bits 3-4).
inline constexpr std::uint8_t kCatFloat    = 0b00;
inline constexpr std::uint8_t kCatSigned   = 0b01;
inline constexpr std::uint8_t kCatUnsigned = 0b10;

inline constexpr std::uint8_t kTagNull      = 0x00;
inline constexpr std::uint8_t kTagBoolFalse = 0x08;
inline constexpr std::uint8_t kTagBoolTrue  = 0x18;

inline std::uint8_t make_header(std::uint8_t type_bits, std::uint8_t category = 0, std::uint8_t width_idx = 0) {
    return std::uint8_t(type_bits & 0b111) | std::uint8_t((category & 0b11) << 3) | std::uint8_t((width_idx & 0b111) << 5);
}

// BEVE is little-endian on the wire; write raw bytes explicitly rather than
// assuming host endianness (mirrors how the rest of zerialize's binary
// protocols treat endianness explicitly, e.g. msgpack.hpp's mp_read_be*).
template <class T>
inline void append_le(std::vector<uint8_t>& out, T v) {
    static_assert(std::is_trivially_copyable_v<T>);
    std::array<std::uint8_t, sizeof(T)> tmp{};
    std::memcpy(tmp.data(), &v, sizeof(T));
    if constexpr (std::endian::native == std::endian::big) {
        std::reverse(tmp.begin(), tmp.end());
    }
    out.insert(out.end(), tmp.begin(), tmp.end());
}

inline void append_raw(std::vector<uint8_t>& out, const void* p, std::size_t n) {
    auto b = reinterpret_cast<const std::uint8_t*>(p);
    out.insert(out.end(), b, b + n);
}

// Compressed unsigned size, as used for string/blob lengths and container
// element counts. See BEVE.md.
inline void write_size(std::vector<uint8_t>& out, std::uint64_t n) {
    if (n < (std::uint64_t(1) << 6)) {
        out.push_back(std::uint8_t(n << 2));
    } else if (n < (std::uint64_t(1) << 14)) {
        append_le<std::uint16_t>(out, std::uint16_t((n << 2) | 1));
    } else if (n < (std::uint64_t(1) << 30)) {
        append_le<std::uint32_t>(out, std::uint32_t((n << 2) | 2));
    } else if (n < (std::uint64_t(1) << 62)) {
        append_le<std::uint64_t>(out, (n << 2) | 3);
    } else {
        throw SerializationError("beve: value too large for compressed size encoding");
    }
}

} // namespace beve_detail

class BeveRootSerializer {
public:
    std::vector<uint8_t> bytes;

    ZBuffer finish() { return ZBuffer(std::move(bytes)); }
};

class BeveSerializer {
    std::vector<uint8_t>& out_;

public:
    explicit BeveSerializer(BeveRootSerializer& rs) : out_(rs.bytes) {}

    void null()          { out_.push_back(beve_detail::kTagNull); }
    void boolean(bool v) { out_.push_back(v ? beve_detail::kTagBoolTrue : beve_detail::kTagBoolFalse); }

    void int64(std::int64_t v) {
        using namespace beve_detail;
        if (v >= std::numeric_limits<std::int8_t>::min() && v <= std::numeric_limits<std::int8_t>::max()) {
            out_.push_back(make_header(kTypeNumber, kCatSigned, 0));
            append_le<std::int8_t>(out_, static_cast<std::int8_t>(v));
        } else if (v >= std::numeric_limits<std::int16_t>::min() && v <= std::numeric_limits<std::int16_t>::max()) {
            out_.push_back(make_header(kTypeNumber, kCatSigned, 1));
            append_le<std::int16_t>(out_, static_cast<std::int16_t>(v));
        } else if (v >= std::numeric_limits<std::int32_t>::min() && v <= std::numeric_limits<std::int32_t>::max()) {
            out_.push_back(make_header(kTypeNumber, kCatSigned, 2));
            append_le<std::int32_t>(out_, static_cast<std::int32_t>(v));
        } else {
            out_.push_back(make_header(kTypeNumber, kCatSigned, 3));
            append_le<std::int64_t>(out_, v);
        }
    }

    void uint64(std::uint64_t v) {
        using namespace beve_detail;
        if (v <= std::numeric_limits<std::uint8_t>::max()) {
            out_.push_back(make_header(kTypeNumber, kCatUnsigned, 0));
            append_le<std::uint8_t>(out_, static_cast<std::uint8_t>(v));
        } else if (v <= std::numeric_limits<std::uint16_t>::max()) {
            out_.push_back(make_header(kTypeNumber, kCatUnsigned, 1));
            append_le<std::uint16_t>(out_, static_cast<std::uint16_t>(v));
        } else if (v <= std::numeric_limits<std::uint32_t>::max()) {
            out_.push_back(make_header(kTypeNumber, kCatUnsigned, 2));
            append_le<std::uint32_t>(out_, static_cast<std::uint32_t>(v));
        } else {
            out_.push_back(make_header(kTypeNumber, kCatUnsigned, 3));
            append_le<std::uint64_t>(out_, v);
        }
    }

    void double_(double v) {
        using namespace beve_detail;
        out_.push_back(make_header(kTypeNumber, kCatFloat, 3)); // float64
        append_le<double>(out_, v);
    }

    void string(std::string_view sv) {
        using namespace beve_detail;
        out_.push_back(make_header(kTypeString));
        write_size(out_, sv.size());
        append_raw(out_, sv.data(), sv.size());
    }

    // Written as a typed array of uint8 -- see the file header comment for
    // why this goes through the same header/size/raw-bytes shape as
    // string(), rather than via a generic tree, and BEVE.md for the wire
    // format this produces.
    void binary(std::span<const std::byte> b) {
        using namespace beve_detail;
        out_.push_back(make_header(kTypeTypedArray, kCatUnsigned, 0)); // uint8 elements
        write_size(out_, b.size());
        append_raw(out_, b.data(), b.size());
    }

    void key(std::string_view k) {
        // Object keys omit the type header (the object header already says
        // "string keys"); just the compressed size + raw UTF-8 bytes.
        beve_detail::write_size(out_, k.size());
        beve_detail::append_raw(out_, k.data(), k.size());
    }

    void begin_map(std::size_t n) {
        out_.push_back(beve_detail::make_header(beve_detail::kTypeObject)); // string-keyed
        beve_detail::write_size(out_, n);
    }
    void end_map() {}

    void begin_array(std::size_t n) {
        out_.push_back(beve_detail::make_header(beve_detail::kTypeGenericArray));
        beve_detail::write_size(out_, n);
    }
    void end_array() {}
};

struct Beve {
    static inline constexpr const char* Name = "Beve";
    using Deserializer   = BeveDeserializer;
    using RootSerializer = BeveRootSerializer;
    using Serializer     = BeveSerializer;
};

} // namespace zerialize
