/*
 * pg_zerialize.cpp
 * PostgreSQL extension for converting rows to binary formats using zerialize
 * Starting with FlexBuffers support
 */

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "catalog/pg_enum.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"
#include "access/tupdesc.h"
#include "executor/spi.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/inval.h"
#include "utils/inet.h"
#include "utils/guc.h"
#include "utils/uuid.h"
#include "mb/pg_wchar.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Builtin final functions used by custom MsgPack aggregate wrappers. */
Datum jsonb_agg_finalfn(PG_FUNCTION_ARGS);
Datum jsonb_object_agg_finalfn(PG_FUNCTION_ARGS);
}

#include <vector>
#include <string>
#include <string_view>
#include <cstring>
#include <array>
#include <exception>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <type_traits>
#include <cmath>
#include <system_error>
#include <charconv>
#include <bit>
#include <limits>
#include <fast_float/fast_float.h>
#include <zerialize/zerialize.hpp>
#include <zerialize/protocols/flex.hpp>
#include <zerialize/protocols/msgpack.hpp>
#include <zerialize/protocols/cbor.hpp>
#include <zerialize/protocols/zera.hpp>
#include <zerialize/dynamic.hpp>
#include <zerialize/internals/base64.hpp>

namespace z = zerialize;

enum NumericFloatBackend {
    NUMERIC_FLOAT_POSTGRES = 0,
    NUMERIC_FLOAT_FAST_FLOAT = 1,
};

static int numeric_float_backend = NUMERIC_FLOAT_FAST_FLOAT;

static const config_enum_entry numeric_float_backend_options[] = {
    {"postgres", NUMERIC_FLOAT_POSTGRES, false},
    {"fast_float", NUMERIC_FLOAT_FAST_FLOAT, false},
    {nullptr, 0, false},
};

/*
 * Forward declarations
 */
extern "C" {
    void _PG_init(void);

    // Single record functions
    Datum row_to_flexbuffers(PG_FUNCTION_ARGS);
    Datum row_to_msgpack(PG_FUNCTION_ARGS);
    Datum row_to_msgpack_slow(PG_FUNCTION_ARGS);
    Datum msgpack_from_jsonb(PG_FUNCTION_ARGS);
    Datum msgpack_to_jsonb(PG_FUNCTION_ARGS);
    Datum msgpack_build_object(PG_FUNCTION_ARGS);
    Datum msgpack_build_array(PG_FUNCTION_ARGS);
    Datum msgpack_agg_final(PG_FUNCTION_ARGS);
    Datum msgpack_object_agg_final(PG_FUNCTION_ARGS);
    Datum row_to_cbor(PG_FUNCTION_ARGS);
    Datum row_to_zera(PG_FUNCTION_ARGS);

    // Batch processing functions
    Datum rows_to_flexbuffers(PG_FUNCTION_ARGS);
    Datum rows_to_msgpack(PG_FUNCTION_ARGS);
    Datum rows_to_msgpack_slow(PG_FUNCTION_ARGS);
    Datum rows_to_cbor(PG_FUNCTION_ARGS);
    Datum rows_to_zera(PG_FUNCTION_ARGS);

    PG_FUNCTION_INFO_V1(row_to_flexbuffers);
    PG_FUNCTION_INFO_V1(row_to_msgpack);
    PG_FUNCTION_INFO_V1(row_to_msgpack_slow);
    PG_FUNCTION_INFO_V1(msgpack_from_jsonb);
    PG_FUNCTION_INFO_V1(msgpack_to_jsonb);
    PG_FUNCTION_INFO_V1(msgpack_build_object);
    PG_FUNCTION_INFO_V1(msgpack_build_array);
    PG_FUNCTION_INFO_V1(msgpack_agg_final);
    PG_FUNCTION_INFO_V1(msgpack_object_agg_final);
    PG_FUNCTION_INFO_V1(row_to_cbor);
    PG_FUNCTION_INFO_V1(row_to_zera);

    PG_FUNCTION_INFO_V1(rows_to_flexbuffers);
    PG_FUNCTION_INFO_V1(rows_to_msgpack);
    PG_FUNCTION_INFO_V1(rows_to_msgpack_slow);
    PG_FUNCTION_INFO_V1(rows_to_cbor);
    PG_FUNCTION_INFO_V1(rows_to_zera);
}

/*
 * Note: The SerializationBuilder class has been removed in favor of direct
 * z::dyn::Value manipulation. See record_to_dynamic_map() and the template
 * functions tuple_to_binary<Protocol> and array_to_binary<Protocol>.
 */

/*
 * Schema caching to avoid repeated TupleDesc lookups
 */
struct TypeCacheKey {
    Oid tupType;
    int32 tupTypmod;

    bool operator==(const TypeCacheKey& other) const {
        return tupType == other.tupType && tupTypmod == other.tupTypmod;
    }
};

// Hash function for cache key
namespace std {
    template<>
    struct hash<TypeCacheKey> {
        size_t operator()(const TypeCacheKey& k) const {
            return hash<Oid>()(k.tupType) ^ (hash<int32>()(k.tupTypmod) << 1);
        }
    };
}

enum class ConverterKind {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Bool,
    Text,
    JsonText,
    Uuid,
    NameText,
    CharText,
    EnumText,
    InetText,
    CidrText,
    IntervalText,
    Numeric,
    Date,
    Timestamp,
    Timestamptz,
    Jsonb,
    Bytea,
    Composite,
    Array,
    Fallback
};

struct CachedColumn;
using MsgpackScalarWriterFn = void (*)(z::MsgPackSerializer&, const CachedColumn&, Datum, bool);
using MsgpackArrayElemWriterFn = void (*)(z::MsgPackSerializer&, Datum, bool);

struct CachedColumn {
    int attnum;
    std::string name;
    std::vector<uint8_t> msgpack_key_encoded;
    std::span<const uint8_t> msgpack_key_view;
    const uint8_t* msgpack_key_ptr;
    size_t msgpack_key_len;
    std::vector<uint8_t> zera_key_encoded;
    MsgpackScalarWriterFn msgpack_scalar_writer;
    MsgpackArrayElemWriterFn msgpack_array_elem_writer;
    Oid typid;
    Oid typoutput;
    ConverterKind kind;
    Oid array_element_typid;
    Oid array_element_typoutput;
    ConverterKind array_element_kind;
    int16 array_typlen;
    bool array_typbyval;
    char array_typalign;
};

struct CachedSchema {
    TupleDesc tupdesc;
    std::vector<CachedColumn> columns;
    std::vector<uint8_t> msgpack_map_header_encoded;
    const uint8_t* msgpack_map_header_ptr;
    size_t msgpack_map_header_len;
    bool use_deform_access;
    bool msgpack_fast_supported;
    bool cbor_fast_supported;
    bool zera_fast_supported;
    bool flex_fast_supported;
};

static bool is_msgpack_fast_kind(ConverterKind kind);
static bool is_msgpack_fast_array_element_kind(ConverterKind kind);
static bool is_msgpack_fast_column(const CachedColumn& col);
static std::vector<uint8_t> encode_msgpack_string_key(std::string_view key);
static std::vector<uint8_t> encode_msgpack_map_header(size_t n);
static std::vector<uint8_t> encode_zera_key(std::string_view key);
static MsgpackScalarWriterFn select_msgpack_scalar_writer(ConverterKind kind);
static MsgpackArrayElemWriterFn select_msgpack_array_elem_writer(ConverterKind kind);
static inline std::span<const std::byte> datum_bytea_span(Datum value);
static inline std::span<const std::byte> datum_jsonb_span(Datum value);
static constexpr size_t kHybridHeapDeformThreshold = 24;

struct TupleDeformScratch {
    static constexpr size_t kInlineCapacity = 64;

private:
    std::array<Datum, kInlineCapacity> inline_values;
    std::array<bool, kInlineCapacity> inline_nulls;
    std::unique_ptr<Datum[]> heap_values;
    std::unique_ptr<bool[]> heap_nulls;

public:
    size_t capacity = kInlineCapacity;
    Datum* values = inline_values.data();
    bool* nulls = inline_nulls.data();

    void ensure(size_t nattrs)
    {
        if (capacity >= nattrs) {
            return;
        }
        heap_values = std::make_unique<Datum[]>(nattrs);
        heap_nulls = std::make_unique<bool[]>(nattrs);
        values = heap_values.get();
        nulls = heap_nulls.get();
        capacity = nattrs;
    }
};

// Global cache for per-schema metadata and TupleDesc lookups.
static std::unordered_map<TypeCacheKey, CachedSchema> schema_cache;
static std::unordered_map<Oid, std::string> enum_label_cache;

static inline void clear_tupdesc_cache()
{
    for (auto& entry : schema_cache) {
        FreeTupleDesc(entry.second.tupdesc);
    }
    schema_cache.clear();
    enum_label_cache.clear();
}

/*
 * Invalidate cached tuple descriptors when catalog/relcache changes occur.
 * This avoids stale descriptors after ALTER TABLE/TYPE.
 */
static void tupdesc_syscache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
    (void)arg;
    (void)cacheid;
    (void)hashvalue;
    clear_tupdesc_cache();
}

static void tupdesc_relcache_callback(Datum arg, Oid relid)
{
    (void)arg;
    (void)relid;
    clear_tupdesc_cache();
}

extern "C" void
_PG_init(void)
{
    DefineCustomEnumVariable(
        "pg_zerialize.numeric_float_backend",
        "Selects the decimal-to-float parser used for non-integral numeric values.",
        nullptr,
        &numeric_float_backend,
        NUMERIC_FLOAT_FAST_FLOAT,
        numeric_float_backend_options,
        PGC_USERSET,
        GUC_NOT_IN_SAMPLE,
        nullptr,
        nullptr,
        nullptr);
    MarkGUCPrefixReserved("pg_zerialize");

    CacheRegisterSyscacheCallback(TYPEOID, tupdesc_syscache_callback, (Datum) 0);
    CacheRegisterSyscacheCallback(RELOID, tupdesc_syscache_callback, (Datum) 0);
    CacheRegisterSyscacheCallback(ATTNUM, tupdesc_syscache_callback, (Datum) 0);
    CacheRegisterSyscacheCallback(ENUMOID, tupdesc_syscache_callback, (Datum) 0);
    CacheRegisterRelcacheCallback(tupdesc_relcache_callback, (Datum) 0);
}

/*
 * Classify PostgreSQL type into converter kind for fast per-column dispatch.
 */
static ConverterKind classify_type(Oid typid)
{
    switch (typid) {
        case INT2OID:
            return ConverterKind::Int2;
        case INT4OID:
            return ConverterKind::Int4;
        case INT8OID:
            return ConverterKind::Int8;
        case FLOAT4OID:
            return ConverterKind::Float4;
        case FLOAT8OID:
            return ConverterKind::Float8;
        case BOOLOID:
            return ConverterKind::Bool;
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
            return ConverterKind::Text;
        case JSONOID:
            return ConverterKind::JsonText;
        case UUIDOID:
            return ConverterKind::Uuid;
        case NAMEOID:
            return ConverterKind::NameText;
        case CHAROID:
            return ConverterKind::CharText;
        case INETOID:
            return ConverterKind::InetText;
        case CIDROID:
            return ConverterKind::CidrText;
        case INTERVALOID:
            return ConverterKind::IntervalText;
        case NUMERICOID:
            return ConverterKind::Numeric;
        case DATEOID:
            return ConverterKind::Date;
        case TIMESTAMPOID:
            return ConverterKind::Timestamp;
        case TIMESTAMPTZOID:
            return ConverterKind::Timestamptz;
        case JSONBOID:
            return ConverterKind::Jsonb;
        case BYTEAOID:
            return ConverterKind::Bytea;
        default:
            if (OidIsValid(get_element_type(typid))) {
                return ConverterKind::Array;
            }
            if (get_typtype(typid) == TYPTYPE_ENUM) {
                return ConverterKind::EnumText;
            }
            if (get_typtype(typid) == TYPTYPE_COMPOSITE) {
                return ConverterKind::Composite;
            }
            return ConverterKind::Fallback;
    }
}

/*
 * Get per-schema metadata with caching to avoid repeated catalog lookups and
 * repeated per-column type classification.
 */
static const CachedSchema& get_cached_schema(Oid tupType, int32 tupTypmod)
{
    TypeCacheKey key{tupType, tupTypmod};

    auto it = schema_cache.find(key);
    if (it != schema_cache.end()) {
        return it->second;
    }

    // Not in cache, look it up
    TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
    TupleDesc cached_tupdesc = CreateTupleDescCopy(tupdesc);
    MemoryContextSwitchTo(old_context);
    ReleaseTupleDesc(tupdesc);

    CachedSchema schema;
    schema.tupdesc = cached_tupdesc;
    schema.use_deform_access = false;
    schema.msgpack_fast_supported = true;
    schema.cbor_fast_supported = true;
    schema.zera_fast_supported = true;
    schema.flex_fast_supported = true;
    schema.columns.reserve(cached_tupdesc->natts);

    for (int i = 0; i < cached_tupdesc->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(cached_tupdesc, i);
        if (att->attisdropped) {
            continue;
        }

        CachedColumn col;
        col.attnum = i + 1;
        col.name = std::string(NameStr(att->attname));
        col.msgpack_key_encoded = encode_msgpack_string_key(col.name);
        col.msgpack_key_view = std::span<const uint8_t>();
        col.msgpack_key_ptr = nullptr;
        col.msgpack_key_len = 0;
        col.zera_key_encoded = encode_zera_key(col.name);
        col.msgpack_scalar_writer = nullptr;
        col.msgpack_array_elem_writer = nullptr;
        col.typid = att->atttypid;
        col.kind = classify_type(col.typid);
        col.msgpack_scalar_writer = select_msgpack_scalar_writer(col.kind);
        col.typoutput = InvalidOid;
        col.array_element_typid = InvalidOid;
        col.array_element_typoutput = InvalidOid;
        col.array_element_kind = ConverterKind::Fallback;
        col.array_typlen = 0;
        col.array_typbyval = false;
        col.array_typalign = 'i';

        if (col.kind == ConverterKind::Array) {
            col.array_element_typid = get_element_type(col.typid);
            if (OidIsValid(col.array_element_typid)) {
                col.array_element_kind = classify_type(col.array_element_typid);
                col.msgpack_array_elem_writer = select_msgpack_array_elem_writer(col.array_element_kind);
                bool element_typisvarlena;
                getTypeOutputInfo(col.array_element_typid,
                                  &col.array_element_typoutput,
                                  &element_typisvarlena);
                get_typlenbyvalalign(col.array_element_typid,
                                    &col.array_typlen,
                                    &col.array_typbyval,
                                    &col.array_typalign);
            }
        }

        if (!is_msgpack_fast_column(col)) {
            schema.msgpack_fast_supported = false;
            schema.cbor_fast_supported = false;
            schema.zera_fast_supported = false;
            schema.flex_fast_supported = false;
        }

        if (col.kind == ConverterKind::Fallback || col.kind == ConverterKind::Array) {
            bool typIsVarlena;
            getTypeOutputInfo(col.typid, &col.typoutput, &typIsVarlena);
        }

        schema.columns.push_back(std::move(col));
        schema.columns.back().msgpack_key_view = schema.columns.back().msgpack_key_encoded;
        schema.columns.back().msgpack_key_ptr = schema.columns.back().msgpack_key_encoded.data();
        schema.columns.back().msgpack_key_len = schema.columns.back().msgpack_key_encoded.size();
    }

    schema.msgpack_map_header_encoded = encode_msgpack_map_header(schema.columns.size());
    schema.msgpack_map_header_ptr = schema.msgpack_map_header_encoded.data();
    schema.msgpack_map_header_len = schema.msgpack_map_header_encoded.size();
    schema.use_deform_access = schema.columns.size() >= kHybridHeapDeformThreshold;

    auto [inserted_it, inserted] = schema_cache.emplace(key, std::move(schema));
    (void)inserted;
    return inserted_it->second;
}

/*
 * Forward declarations for conversion functions
 */
static z::dyn::Value datum_to_dynamic(Datum value, Oid typid, bool isnull);
static z::dyn::Value record_to_dynamic_map(HeapTupleHeader rec);
static bytea* try_serialize_msgpack_row_fast(HeapTupleHeader rec);
static bytea* try_serialize_msgpack_array_fast(Datum* elements, bool* nulls, int nitems);
static bytea* try_serialize_cbor_row_fast(HeapTupleHeader rec);
static bytea* try_serialize_cbor_array_fast(Datum* elements, bool* nulls, int nitems);
static bytea* try_serialize_zera_row_fast(HeapTupleHeader rec);
static bytea* try_serialize_zera_array_fast(Datum* elements, bool* nulls, int nitems);
static bytea* try_serialize_flex_row_fast(HeapTupleHeader rec);
static bytea* try_serialize_flex_array_fast(Datum* elements, bool* nulls, int nitems);

static z::dyn::Value array_level_to_dynamic(
    Datum* elements,
    bool* nulls,
    Oid element_type,
    const int* dims,
    int ndim,
    int depth,
    int* offset)
{
    z::dyn::Value::Array result;
    result.reserve(dims[depth]);

    if (depth == ndim - 1) {
        for (int i = 0; i < dims[depth]; i++, (*offset)++) {
            result.push_back(datum_to_dynamic(
                elements[*offset], element_type, nulls[*offset]));
        }
    } else {
        for (int i = 0; i < dims[depth]; i++) {
            result.push_back(array_level_to_dynamic(
                elements, nulls, element_type, dims, ndim, depth + 1, offset));
        }
    }

    return z::dyn::Value::array(std::move(result));
}

/*
 * Convert a PostgreSQL array to a zerialize dyn::Value array
 */
static z::dyn::Value array_to_dynamic(Datum value, Oid typid)
{
    (void) typid;
    ArrayType* arr = DatumGetArrayTypeP(value);
    int ndim = ARR_NDIM(arr);

    // Handle empty arrays
    if (ndim == 0 || ArrayGetNItems(ndim, ARR_DIMS(arr)) == 0) {
        return z::dyn::Value::array(z::dyn::Value::Array());
    }

    // Get array element type and info
    Oid element_type = ARR_ELEMTYPE(arr);
    int16 typlen;
    bool typbyval;
    char typalign;
    get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

    // Deconstruct array into elements
    Datum* elements;
    bool* nulls;
    int nitems;

    deconstruct_array(arr, element_type, typlen, typbyval, typalign,
                     &elements, &nulls, &nitems);

    int offset = 0;
    z::dyn::Value result = array_level_to_dynamic(
        elements, nulls, element_type, ARR_DIMS(arr), ndim, 0, &offset);

    pfree(elements);
    pfree(nulls);

    return result;
}

struct NumericFastValue {
    bool is_int64;
    int64_t int64_value;
    double float8_value;
};

static inline bool numeric_text_try_int64(
    const char* begin, const char* end, int64_t* out)
{
    const char* integer_end = end;
    const char* decimal = static_cast<const char*>(
        std::memchr(begin, '.', static_cast<size_t>(end - begin)));
    if (decimal != nullptr) {
        for (const char* p = decimal + 1; p < end; p++) {
            if (*p != '0') {
                return false;
            }
        }
        integer_end = decimal;
    }

    auto parsed = std::from_chars(begin, integer_end, *out);
    return parsed.ec == std::errc() && parsed.ptr == integer_end;
}

static inline NumericFastValue numeric_parse_fast(Datum value)
{
    char* text = DatumGetCString(DirectFunctionCall1(numeric_out, value));
    const char* end = text + std::strlen(text);
    int64_t int64_value;
    if (numeric_text_try_int64(text, end, &int64_value)) {
        pfree(text);
        return NumericFastValue{true, int64_value, 0.0};
    }

    if (numeric_float_backend == NUMERIC_FLOAT_FAST_FLOAT) {
        double result;
        auto parsed = fast_float::from_chars(text, end, result);
        bool valid = parsed.ec == std::errc() && parsed.ptr == end;
        pfree(text);

        if (valid) {
            return NumericFastValue{false, 0, result};
        }
        // Preserve PostgreSQL's overflow and special-value behavior.
    } else {
        pfree(text);
    }

    Datum float_val = DirectFunctionCall1(numeric_float8, value);
    return NumericFastValue{false, 0, DatumGetFloat8(float_val)};
}

template <typename WriterT>
static inline void numeric_write_fast(WriterT& writer, Datum value)
{
    NumericFastValue parsed = numeric_parse_fast(value);
    if (parsed.is_int64) {
        writer.int64(parsed.int64_value);
    } else {
        writer.double_(parsed.float8_value);
    }
}

static inline z::dyn::Value numeric_to_dynamic_fast(Datum value)
{
    NumericFastValue parsed = numeric_parse_fast(value);
    if (parsed.is_int64) {
        return z::dyn::Value(parsed.int64_value);
    }
    return z::dyn::Value(parsed.float8_value);
}

static z::dyn::Value jsonb_token_to_dynamic(JsonbIterator** it, JsonbIteratorToken tok, JsonbValue* v);

static z::dyn::Value jsonb_scalar_to_dynamic(const JsonbValue& v)
{
    switch (v.type) {
        case jbvNull:
            return z::dyn::Value();
        case jbvBool:
            return z::dyn::Value(v.val.boolean);
        case jbvNumeric:
            return numeric_to_dynamic_fast(NumericGetDatum(v.val.numeric));
        case jbvString:
            return z::dyn::Value(std::string(v.val.string.val, v.val.string.len));
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("unsupported jsonb scalar type for msgpack conversion")));
    }
}

static z::dyn::Value jsonb_token_to_dynamic(JsonbIterator** it, JsonbIteratorToken tok, JsonbValue* v)
{
    if (tok == WJB_BEGIN_OBJECT) {
        z::dyn::Value::Map map_entries;
        JsonbIteratorToken t = JsonbIteratorNext(it, v, false);
        while (t != WJB_END_OBJECT) {
            if (t != WJB_KEY) {
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED),
                         errmsg("invalid jsonb object token sequence")));
            }
            std::string key(v->val.string.val, v->val.string.len);
            t = JsonbIteratorNext(it, v, false);
            map_entries.emplace_back(std::move(key), jsonb_token_to_dynamic(it, t, v));
            t = JsonbIteratorNext(it, v, false);
        }
        return z::dyn::Value::map(std::move(map_entries));
    }

    if (tok == WJB_BEGIN_ARRAY) {
        z::dyn::Value::Array arr;
        JsonbIteratorToken t = JsonbIteratorNext(it, v, false);
        while (t != WJB_END_ARRAY) {
            arr.push_back(jsonb_token_to_dynamic(it, t, v));
            t = JsonbIteratorNext(it, v, false);
        }
        return z::dyn::Value::array(std::move(arr));
    }

    if (tok == WJB_VALUE || tok == WJB_ELEM) {
        return jsonb_scalar_to_dynamic(*v);
    }

    ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
             errmsg("invalid jsonb token for conversion")));
}

static inline void msgpack_require_bytes(
    std::span<const uint8_t> data, size_t pos, size_t count)
{
    if (pos > data.size() || count > data.size() - pos) {
        throw z::DeserializationError("truncated MessagePack value");
    }
}

static inline uint16_t msgpack_read_u16(std::span<const uint8_t> data, size_t pos)
{
    msgpack_require_bytes(data, pos, 2);
    return static_cast<uint16_t>(
        (static_cast<uint16_t>(data[pos]) << 8) | data[pos + 1]);
}

static inline uint32_t msgpack_read_u32(std::span<const uint8_t> data, size_t pos)
{
    msgpack_require_bytes(data, pos, 4);
    return (static_cast<uint32_t>(data[pos]) << 24) |
           (static_cast<uint32_t>(data[pos + 1]) << 16) |
           (static_cast<uint32_t>(data[pos + 2]) << 8) |
           static_cast<uint32_t>(data[pos + 3]);
}

static inline bool msgpack_marker_is_string(uint8_t marker)
{
    return (marker & 0xe0) == 0xa0 || marker == 0xd9 ||
           marker == 0xda || marker == 0xdb;
}

static size_t msgpack_validate_value(std::span<const uint8_t> data, size_t pos)
{
    check_stack_depth();
    msgpack_require_bytes(data, pos, 1);
    const uint8_t marker = data[pos++];

    if (marker <= 0x7f || marker >= 0xe0 || marker == 0xc0 ||
        marker == 0xc2 || marker == 0xc3) {
        return pos;
    }

    uint64_t count = 0;
    if ((marker & 0xe0) == 0xa0) {
        count = marker & 0x1f;
        msgpack_require_bytes(data, pos, static_cast<size_t>(count));
        return pos + static_cast<size_t>(count);
    }
    if ((marker & 0xf0) == 0x90) {
        count = marker & 0x0f;
        for (uint64_t i = 0; i < count; i++) {
            pos = msgpack_validate_value(data, pos);
        }
        return pos;
    }
    if ((marker & 0xf0) == 0x80) {
        count = marker & 0x0f;
        std::unordered_set<std::string_view> keys;
        for (uint64_t i = 0; i < count; i++) {
            msgpack_require_bytes(data, pos, 1);
            if (!msgpack_marker_is_string(data[pos])) {
                throw z::DeserializationError("MessagePack map key is not a string");
            }
            const size_t key_start = pos;
            pos = msgpack_validate_value(data, pos);
            z::MsgPackDeserializer key_reader(
                data.subspan(key_start, pos - key_start));
            if (!keys.insert(key_reader.asStringView()).second) {
                throw z::DeserializationError("duplicate MessagePack map key");
            }
            pos = msgpack_validate_value(data, pos);
        }
        return pos;
    }

    size_t payload_size = 0;
    switch (marker) {
        case 0xc4:
        case 0xd9:
            msgpack_require_bytes(data, pos, 1);
            payload_size = data[pos++];
            break;
        case 0xc5:
        case 0xda:
            payload_size = msgpack_read_u16(data, pos);
            pos += 2;
            break;
        case 0xc6:
        case 0xdb:
            payload_size = msgpack_read_u32(data, pos);
            pos += 4;
            break;
        case 0xca:
        case 0xce:
        case 0xd2:
            payload_size = 4;
            break;
        case 0xcb:
        case 0xcf:
        case 0xd3:
            payload_size = 8;
            break;
        case 0xcc:
        case 0xd0:
            payload_size = 1;
            break;
        case 0xcd:
        case 0xd1:
            payload_size = 2;
            break;
        case 0xdc:
            count = msgpack_read_u16(data, pos);
            pos += 2;
            for (uint64_t i = 0; i < count; i++) {
                pos = msgpack_validate_value(data, pos);
            }
            return pos;
        case 0xdd:
            count = msgpack_read_u32(data, pos);
            pos += 4;
            for (uint64_t i = 0; i < count; i++) {
                pos = msgpack_validate_value(data, pos);
            }
            return pos;
        case 0xde:
        case 0xdf:
            if (marker == 0xde) {
                count = msgpack_read_u16(data, pos);
                pos += 2;
            } else {
                count = msgpack_read_u32(data, pos);
                pos += 4;
            }
            {
                std::unordered_set<std::string_view> keys;
                for (uint64_t i = 0; i < count; i++) {
                    msgpack_require_bytes(data, pos, 1);
                    if (!msgpack_marker_is_string(data[pos])) {
                        throw z::DeserializationError("MessagePack map key is not a string");
                    }
                    const size_t key_start = pos;
                    pos = msgpack_validate_value(data, pos);
                    z::MsgPackDeserializer key_reader(
                        data.subspan(key_start, pos - key_start));
                    if (!keys.insert(key_reader.asStringView()).second) {
                        throw z::DeserializationError("duplicate MessagePack map key");
                    }
                    pos = msgpack_validate_value(data, pos);
                }
            }
            return pos;
        default:
            throw z::DeserializationError("unsupported or reserved MessagePack marker");
    }

    msgpack_require_bytes(data, pos, payload_size);
    return pos + payload_size;
}

static void append_json_string(std::string& out, std::string_view value)
{
    if (value.find('\0') != std::string_view::npos) {
        throw z::DeserializationError("MessagePack string contains NUL");
    }
    if (!pg_verify_mbstr(GetDatabaseEncoding(), value.data(), value.size(), true)) {
        throw z::DeserializationError(
            "MessagePack string is invalid in the database encoding");
    }
    static constexpr char hex[] = "0123456789abcdef";
    out.push_back('"');
    for (unsigned char ch : value) {
        switch (ch) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (ch < 0x20) {
                    out += "\\u00";
                    out.push_back(hex[ch >> 4]);
                    out.push_back(hex[ch & 0x0f]);
                } else {
                    out.push_back(static_cast<char>(ch));
                }
        }
    }
    out.push_back('"');
}

template<typename Number>
static void append_json_number(std::string& out, Number value)
{
    std::array<char, 64> buffer;
    std::to_chars_result converted;
    if constexpr (std::is_floating_point_v<Number>) {
        converted = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value,
                                  std::chars_format::general,
                                  std::numeric_limits<Number>::max_digits10);
    } else {
        converted = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    }
    if (converted.ec != std::errc()) {
        throw z::DeserializationError("failed to format MessagePack number");
    }
    out.append(buffer.data(), converted.ptr);
}

static void msgpack_reader_to_json(
    const z::MsgPackDeserializer& value, std::string& out)
{
    check_stack_depth();
    if (value.isNull()) {
        out += "null";
    } else if (value.isBool()) {
        out += value.asBool() ? "true" : "false";
    } else if (value.isUInt()) {
        append_json_number(out, value.asUInt64());
    } else if (value.isInt()) {
        append_json_number(out, value.asInt64());
    } else if (value.isFloat()) {
        const double number = value.asDouble();
        if (std::isfinite(number)) {
            append_json_number(out, number);
        } else if (std::isnan(number)) {
            append_json_string(out, "NaN");
        } else {
            append_json_string(out, number > 0 ? "Infinity" : "-Infinity");
        }
    } else if (value.isString()) {
        append_json_string(out, value.asStringView());
    } else if (value.isBlob()) {
        out += "[\"~b\",";
        append_json_string(out, z::base64Encode(value.asBlob()));
        out += ",\"base64\"]";
    } else if (value.isArray()) {
        out.push_back('[');
        const size_t count = value.arraySize();
        for (size_t i = 0; i < count; i++) {
            if (i > 0) out.push_back(',');
            msgpack_reader_to_json(value[i], out);
        }
        out.push_back(']');
    } else if (value.isMap()) {
        out.push_back('{');
        bool first = true;
        for (std::string_view key : value.mapKeys()) {
            if (!first) out.push_back(',');
            first = false;
            append_json_string(out, key);
            out.push_back(':');
            msgpack_reader_to_json(value[key], out);
        }
        out.push_back('}');
    } else {
        throw z::DeserializationError("unsupported MessagePack value");
    }
}

static z::dyn::Value jsonb_to_dynamic(Jsonb* jb)
{
    JsonbIterator* it = JsonbIteratorInit(&jb->root);
    JsonbValue v;

    if (JB_ROOT_IS_SCALAR(jb)) {
        JsonbIteratorToken tok = JsonbIteratorNext(&it, &v, false); /* begin pseudo-array */
        if (tok != WJB_BEGIN_ARRAY) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("invalid scalar jsonb root")));
        }
        tok = JsonbIteratorNext(&it, &v, false); /* scalar elem */
        z::dyn::Value out = jsonb_token_to_dynamic(&it, tok, &v);
        (void) JsonbIteratorNext(&it, &v, false); /* end pseudo-array */
        return out;
    }

    JsonbIteratorToken tok = JsonbIteratorNext(&it, &v, false);
    return jsonb_token_to_dynamic(&it, tok, &v);
}

static inline std::string text_to_owned_string(Datum value)
{
    text* txt = DatumGetTextPP(value);
    return std::string(VARDATA_ANY(txt), static_cast<size_t>(VARSIZE_ANY_EXHDR(txt)));
}

static inline void format_uuid(Datum value, char (&out)[36])
{
    static constexpr char hex[] = "0123456789abcdef";
    const pg_uuid_t* uuid = DatumGetUUIDP(value);
    size_t pos = 0;
    for (size_t i = 0; i < UUID_LEN; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            out[pos++] = '-';
        }
        out[pos++] = hex[uuid->data[i] >> 4];
        out[pos++] = hex[uuid->data[i] & 0x0f];
    }
}

static inline std::string uuid_to_owned_string(Datum value)
{
    char out[36];
    format_uuid(value, out);
    return std::string(out, sizeof(out));
}

static inline std::string_view name_text_view(Datum value)
{
    const char* name = NameStr(*DatumGetName(value));
    return std::string_view(name, strnlen(name, NAMEDATALEN));
}

static inline std::string char_to_owned_string(Datum value)
{
    char ch = DatumGetChar(value);
    return ch == '\0' ? std::string() : std::string(1, ch);
}

static inline std::string_view enum_label_view(Datum value)
{
    Oid enum_value = DatumGetObjectId(value);
    auto cached = enum_label_cache.find(enum_value);
    if (cached != enum_label_cache.end()) {
        return cached->second;
    }

    HeapTuple tuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(enum_value));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("invalid internal value for enum: %u", enum_value)));
    }
    Form_pg_enum entry = (Form_pg_enum) GETSTRUCT(tuple);
    auto [it, inserted] = enum_label_cache.emplace(enum_value, NameStr(entry->enumlabel));
    (void)inserted;
    ReleaseSysCache(tuple);
    return it->second;
}

static constexpr size_t kNetworkTextCapacity =
    sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128");

static inline std::string_view network_text_view(
    Datum value, bool is_cidr, char (&out)[kNetworkTextCapacity])
{
    inet* src = DatumGetInetPP(value);
    if (pg_inet_net_ntop(ip_family(src), ip_addr(src), ip_bits(src),
                         out, sizeof(out)) == nullptr) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("could not format inet value")));
    }

    size_t len = strlen(out);
    if (is_cidr && strchr(out, '/') == nullptr) {
        int written = snprintf(out + len, sizeof(out) - len, "/%u", ip_bits(src));
        if (written < 0 || static_cast<size_t>(written) >= sizeof(out) - len) {
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("formatted cidr value exceeds buffer")));
        }
        len += static_cast<size_t>(written);
    }
    return std::string_view(out, len);
}

template <typename WriterT>
static inline void write_network_string(WriterT& writer, Datum value, bool is_cidr)
{
    char out[kNetworkTextCapacity];
    writer.string(network_text_view(value, is_cidr, out));
}

static inline std::string_view interval_text_view(
    Datum value, char (&out)[MAXDATELEN + 1])
{
    Interval* span = DatumGetIntervalP(value);
#if PG_VERSION_NUM >= 170000
    if (INTERVAL_IS_NOBEGIN(span)) {
        return std::string_view(EARLY, sizeof(EARLY) - 1);
    }
    if (INTERVAL_IS_NOEND(span)) {
        return std::string_view(LATE, sizeof(LATE) - 1);
    }
#endif

    struct pg_itm itm;
    interval2itm(*span, &itm);
    EncodeInterval(&itm, IntervalStyle, out);
    return std::string_view(out, strlen(out));
}

template <typename WriterT>
static inline void write_interval_string(WriterT& writer, Datum value)
{
    char out[MAXDATELEN + 1];
    writer.string(interval_text_view(value, out));
}

template <typename WriterT>
static inline void write_output_string(WriterT& writer, Oid typoutput, Datum value)
{
    char* str = OidOutputFunctionCall(typoutput, value);
    writer.string(std::string_view(str));
    pfree(str);
}

/*
 * Convert a PostgreSQL Datum to a zerialize dyn::Value
 */
static z::dyn::Value datum_to_dynamic(Datum value, Oid typid, bool isnull)
{
    if (isnull) {
        return z::dyn::Value();  // null value
    }

    // Check if this is an array type
    if (OidIsValid(get_element_type(typid))) {
        return array_to_dynamic(value, typid);
    }

    switch (typid) {
        case INT2OID:
            return z::dyn::Value(static_cast<int64_t>(DatumGetInt16(value)));

        case INT4OID:
            return z::dyn::Value(static_cast<int64_t>(DatumGetInt32(value)));

        case INT8OID:
            return z::dyn::Value(static_cast<int64_t>(DatumGetInt64(value)));

        case FLOAT4OID:
            return z::dyn::Value(static_cast<double>(DatumGetFloat4(value)));

        case FLOAT8OID:
            return z::dyn::Value(DatumGetFloat8(value));

        case BOOLOID:
            return z::dyn::Value(DatumGetBool(value));

        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
        case JSONOID:
        {
            return z::dyn::Value(text_to_owned_string(value));
        }

        case UUIDOID:
            return z::dyn::Value(uuid_to_owned_string(value));
        case NAMEOID:
            return z::dyn::Value(std::string(name_text_view(value)));
        case CHAROID:
            return z::dyn::Value(char_to_owned_string(value));

        case NUMERICOID:
            return numeric_to_dynamic_fast(value);
        case DATEOID:
            return z::dyn::Value(static_cast<int64_t>(DatumGetDateADT(value)));
        case TIMESTAMPOID:
            return z::dyn::Value(static_cast<int64_t>(DatumGetTimestamp(value)));
        case TIMESTAMPTZOID:
            return z::dyn::Value(static_cast<int64_t>(DatumGetTimestampTz(value)));
        case JSONBOID:
            return z::dyn::Value::blob(datum_jsonb_span(value));
        case BYTEAOID:
            return z::dyn::Value::blob(datum_bytea_span(value));

        default:
        {
            if (typid == RECORDOID || get_typtype(typid) == TYPTYPE_COMPOSITE) {
                return record_to_dynamic_map(DatumGetHeapTupleHeader(value));
            }

            // For unsupported types, convert to text representation
            Oid typoutput;
            bool typIsVarlena;
            char* str;

            getTypeOutputInfo(typid, &typoutput, &typIsVarlena);
            str = OidOutputFunctionCall(typoutput, value);
            std::string strval(str);
            pfree(str);
            return z::dyn::Value(strval);
        }
    }
}

static z::dyn::Value fallback_to_text_output(Datum value, Oid typoutput)
{
    char* str = OidOutputFunctionCall(typoutput, value);
    std::string strval(str);
    pfree(str);
    return z::dyn::Value(strval);
}

static std::string datum_to_string_output(Datum value, Oid typid)
{
    Oid typoutput;
    bool typIsVarlena;
    getTypeOutputInfo(typid, &typoutput, &typIsVarlena);
    char* str = OidOutputFunctionCall(typoutput, value);
    std::string out(str);
    pfree(str);
    return out;
}

static inline std::span<const std::byte> datum_bytea_span(Datum value)
{
    bytea* b = DatumGetByteaPP(value);
    const std::byte* ptr = reinterpret_cast<const std::byte*>(VARDATA_ANY(b));
    size_t len = static_cast<size_t>(VARSIZE_ANY_EXHDR(b));
    return std::span<const std::byte>(ptr, len);
}

static inline std::span<const std::byte> datum_jsonb_span(Datum value)
{
    Jsonb* jb = DatumGetJsonbP(value);
    const std::byte* ptr = reinterpret_cast<const std::byte*>(VARDATA_ANY(jb));
    size_t len = static_cast<size_t>(VARSIZE_ANY_EXHDR(jb));
    return std::span<const std::byte>(ptr, len);
}

static z::dyn::Value datum_to_dynamic_cached(Datum value, bool isnull, const CachedColumn& col)
{
    if (isnull) {
        return z::dyn::Value();
    }

    switch (col.kind) {
        case ConverterKind::Int2:
            return z::dyn::Value(static_cast<int64_t>(DatumGetInt16(value)));
        case ConverterKind::Int4:
            return z::dyn::Value(static_cast<int64_t>(DatumGetInt32(value)));
        case ConverterKind::Int8:
            return z::dyn::Value(static_cast<int64_t>(DatumGetInt64(value)));
        case ConverterKind::Float4:
            return z::dyn::Value(static_cast<double>(DatumGetFloat4(value)));
        case ConverterKind::Float8:
            return z::dyn::Value(DatumGetFloat8(value));
        case ConverterKind::Bool:
            return z::dyn::Value(DatumGetBool(value));
        case ConverterKind::Text:
        case ConverterKind::JsonText:
        {
            return z::dyn::Value(text_to_owned_string(value));
        }
        case ConverterKind::Uuid:
            return z::dyn::Value(uuid_to_owned_string(value));
        case ConverterKind::NameText:
            return z::dyn::Value(std::string(name_text_view(value)));
        case ConverterKind::CharText:
            return z::dyn::Value(char_to_owned_string(value));
        case ConverterKind::EnumText:
            return z::dyn::Value(std::string(enum_label_view(value)));
        case ConverterKind::InetText:
        case ConverterKind::CidrText:
        {
            char out[kNetworkTextCapacity];
            return z::dyn::Value(std::string(network_text_view(
                value, col.kind == ConverterKind::CidrText, out)));
        }
        case ConverterKind::IntervalText:
        {
            char out[MAXDATELEN + 1];
            return z::dyn::Value(std::string(interval_text_view(value, out)));
        }
        case ConverterKind::Numeric:
            return numeric_to_dynamic_fast(value);
        case ConverterKind::Date:
            return z::dyn::Value(static_cast<int64_t>(DatumGetDateADT(value)));
        case ConverterKind::Timestamp:
            return z::dyn::Value(static_cast<int64_t>(DatumGetTimestamp(value)));
        case ConverterKind::Timestamptz:
            return z::dyn::Value(static_cast<int64_t>(DatumGetTimestampTz(value)));
        case ConverterKind::Jsonb:
            return z::dyn::Value::blob(datum_jsonb_span(value));
        case ConverterKind::Bytea:
            return z::dyn::Value::blob(datum_bytea_span(value));
        case ConverterKind::Array:
            return array_to_dynamic(value, col.typid);
        case ConverterKind::Composite:
            return record_to_dynamic_map(DatumGetHeapTupleHeader(value));
        case ConverterKind::Fallback:
            return fallback_to_text_output(value, col.typoutput);
    }

    return z::dyn::Value();
}

static bool is_msgpack_fast_kind(ConverterKind kind)
{
    switch (kind) {
        case ConverterKind::Int2:
        case ConverterKind::Int4:
        case ConverterKind::Int8:
        case ConverterKind::Float4:
        case ConverterKind::Float8:
        case ConverterKind::Bool:
        case ConverterKind::Text:
        case ConverterKind::JsonText:
        case ConverterKind::Uuid:
        case ConverterKind::NameText:
        case ConverterKind::CharText:
        case ConverterKind::EnumText:
        case ConverterKind::InetText:
        case ConverterKind::CidrText:
        case ConverterKind::IntervalText:
        case ConverterKind::Numeric:
        case ConverterKind::Date:
        case ConverterKind::Timestamp:
        case ConverterKind::Timestamptz:
        case ConverterKind::Jsonb:
        case ConverterKind::Bytea:
        case ConverterKind::Fallback:
            return true;
        default:
            return false;
    }
}

static bool is_msgpack_fast_array_element_kind(ConverterKind kind)
{
    switch (kind) {
        case ConverterKind::Int2:
        case ConverterKind::Int4:
        case ConverterKind::Int8:
        case ConverterKind::Float4:
        case ConverterKind::Float8:
        case ConverterKind::Bool:
        case ConverterKind::Text:
        case ConverterKind::JsonText:
        case ConverterKind::Uuid:
        case ConverterKind::NameText:
        case ConverterKind::CharText:
        case ConverterKind::EnumText:
        case ConverterKind::InetText:
        case ConverterKind::CidrText:
        case ConverterKind::IntervalText:
        case ConverterKind::Numeric:
        case ConverterKind::Date:
        case ConverterKind::Timestamp:
        case ConverterKind::Timestamptz:
        case ConverterKind::Jsonb:
        case ConverterKind::Bytea:
        case ConverterKind::Fallback:
            return true;
        default:
            return false;
    }
}

static bool is_msgpack_fast_column(const CachedColumn& col)
{
    if (col.kind != ConverterKind::Array) {
        return is_msgpack_fast_kind(col.kind);
    }
    return OidIsValid(col.array_element_typid) &&
           is_msgpack_fast_array_element_kind(col.array_element_kind);
}

static std::vector<uint8_t> encode_msgpack_string_key(std::string_view key)
{
    std::vector<uint8_t> out;
    const size_t n = key.size();

    if (n <= 31) {
        out.reserve(1 + n);
        out.push_back(static_cast<uint8_t>(0xA0u | static_cast<uint8_t>(n)));
    } else if (n <= 0xFFu) {
        out.reserve(2 + n);
        out.push_back(0xD9u);
        out.push_back(static_cast<uint8_t>(n));
    } else if (n <= 0xFFFFu) {
        out.reserve(3 + n);
        out.push_back(0xDAu);
        out.push_back(static_cast<uint8_t>((n >> 8) & 0xFFu));
        out.push_back(static_cast<uint8_t>(n & 0xFFu));
    } else if (n <= 0xFFFFFFFFu) {
        out.reserve(5 + n);
        out.push_back(0xDBu);
        out.push_back(static_cast<uint8_t>((n >> 24) & 0xFFu));
        out.push_back(static_cast<uint8_t>((n >> 16) & 0xFFu));
        out.push_back(static_cast<uint8_t>((n >> 8) & 0xFFu));
        out.push_back(static_cast<uint8_t>(n & 0xFFu));
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("map key length exceeds MessagePack limits")));
    }

    out.insert(out.end(), key.begin(), key.end());
    return out;
}

static std::vector<uint8_t> encode_msgpack_map_header(size_t n)
{
    std::vector<uint8_t> out;
    if (n <= 15u) {
        out.reserve(1);
        out.push_back(static_cast<uint8_t>(0x80u | static_cast<uint8_t>(n)));
        return out;
    }

    if (n <= 0xFFFFu) {
        out.reserve(3);
        out.push_back(0xDEu);
        out.push_back(static_cast<uint8_t>((n >> 8) & 0xFFu));
        out.push_back(static_cast<uint8_t>(n & 0xFFu));
        return out;
    }

    if (n <= 0xFFFFFFFFu) {
        out.reserve(5);
        out.push_back(0xDFu);
        out.push_back(static_cast<uint8_t>((n >> 24) & 0xFFu));
        out.push_back(static_cast<uint8_t>((n >> 16) & 0xFFu));
        out.push_back(static_cast<uint8_t>((n >> 8) & 0xFFu));
        out.push_back(static_cast<uint8_t>(n & 0xFFu));
        return out;
    }

    ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg("map field count exceeds MessagePack limits")));
}

static std::vector<uint8_t> encode_zera_key(std::string_view key)
{
    if (key.size() > 0xFFFFu) {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("map key length exceeds ZERA key limit")));
    }

    std::vector<uint8_t> out;
    out.reserve(4 + key.size());
    const uint16_t len = static_cast<uint16_t>(key.size());
    out.push_back(static_cast<uint8_t>(len & 0xFFu));
    out.push_back(static_cast<uint8_t>((len >> 8) & 0xFFu));
    out.push_back(0);
    out.push_back(0);
    out.insert(out.end(), key.begin(), key.end());
    return out;
}

static inline void msgpack_write_text(z::MsgPackSerializer& writer, Datum value)
{
    text* txt = DatumGetTextPP(value);
    const char* ptr = VARDATA_ANY(txt);
    int len = VARSIZE_ANY_EXHDR(txt);
    writer.string(std::string_view(ptr, static_cast<size_t>(len)));
}

static inline void msgpack_array_elem_unsupported(
    z::MsgPackSerializer& writer,
    Datum value,
    bool isnull)
{
    (void)writer;
    (void)value;
    (void)isnull;
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("unsupported array element type for fast MessagePack path")));
}

static inline void msgpack_array_elem_int2(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetInt16(value)));
}

static inline void msgpack_array_elem_int4(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetInt32(value)));
}

static inline void msgpack_array_elem_int8(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetInt64(value)));
}

static inline void msgpack_array_elem_float4(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.double_(static_cast<double>(DatumGetFloat4(value)));
}

static inline void msgpack_array_elem_float8(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.double_(DatumGetFloat8(value));
}

static inline void msgpack_array_elem_bool(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.boolean(DatumGetBool(value));
}

static inline void msgpack_array_elem_text(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    msgpack_write_text(writer, value);
}

static inline void msgpack_array_elem_uuid(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    char out[36];
    format_uuid(value, out);
    writer.string(std::string_view(out, sizeof(out)));
}

static inline void msgpack_array_elem_name(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.string(name_text_view(value));
}

static inline void msgpack_array_elem_char(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    char ch = DatumGetChar(value);
    writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
}

static inline void msgpack_array_elem_enum(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.string(enum_label_view(value));
}

static inline void msgpack_array_elem_inet(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    write_network_string(writer, value, false);
}

static inline void msgpack_array_elem_cidr(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    write_network_string(writer, value, true);
}

static inline void msgpack_array_elem_interval(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    write_interval_string(writer, value);
}

static inline void msgpack_array_elem_numeric(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    numeric_write_fast(writer, value);
}

static inline void msgpack_array_elem_date(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetDateADT(value)));
}

static inline void msgpack_array_elem_timestamp(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetTimestamp(value)));
}

static inline void msgpack_array_elem_timestamptz(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetTimestampTz(value)));
}

static inline void msgpack_array_elem_jsonb(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.binary(datum_jsonb_span(value));
}

static inline void msgpack_array_elem_bytea(z::MsgPackSerializer& writer, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.binary(datum_bytea_span(value));
}

static MsgpackArrayElemWriterFn select_msgpack_array_elem_writer(ConverterKind kind)
{
    switch (kind) {
        case ConverterKind::Int2: return &msgpack_array_elem_int2;
        case ConverterKind::Int4: return &msgpack_array_elem_int4;
        case ConverterKind::Int8: return &msgpack_array_elem_int8;
        case ConverterKind::Float4: return &msgpack_array_elem_float4;
        case ConverterKind::Float8: return &msgpack_array_elem_float8;
        case ConverterKind::Bool: return &msgpack_array_elem_bool;
        case ConverterKind::Text:
        case ConverterKind::JsonText:
            return &msgpack_array_elem_text;
        case ConverterKind::Uuid: return &msgpack_array_elem_uuid;
        case ConverterKind::NameText: return &msgpack_array_elem_name;
        case ConverterKind::CharText: return &msgpack_array_elem_char;
        case ConverterKind::EnumText: return &msgpack_array_elem_enum;
        case ConverterKind::InetText: return &msgpack_array_elem_inet;
        case ConverterKind::CidrText: return &msgpack_array_elem_cidr;
        case ConverterKind::IntervalText: return &msgpack_array_elem_interval;
        case ConverterKind::Numeric: return &msgpack_array_elem_numeric;
        case ConverterKind::Date: return &msgpack_array_elem_date;
        case ConverterKind::Timestamp: return &msgpack_array_elem_timestamp;
        case ConverterKind::Timestamptz: return &msgpack_array_elem_timestamptz;
        case ConverterKind::Jsonb: return &msgpack_array_elem_jsonb;
        case ConverterKind::Bytea: return &msgpack_array_elem_bytea;
        default: return &msgpack_array_elem_unsupported;
    }
}

static inline void msgpack_write_array_no_nulls(
    z::MsgPackSerializer& writer,
    ConverterKind elem_kind,
    Datum* elements,
    int nitems)
{
    switch (elem_kind) {
        case ConverterKind::Int2:
            for (int i = 0; i < nitems; i++) {
                writer.int64(static_cast<int64_t>(DatumGetInt16(elements[i])));
            }
            return;
        case ConverterKind::Int4:
            for (int i = 0; i < nitems; i++) {
                writer.int64(static_cast<int64_t>(DatumGetInt32(elements[i])));
            }
            return;
        case ConverterKind::Int8:
            for (int i = 0; i < nitems; i++) {
                writer.int64(static_cast<int64_t>(DatumGetInt64(elements[i])));
            }
            return;
        case ConverterKind::Float4:
            for (int i = 0; i < nitems; i++) {
                writer.double_(static_cast<double>(DatumGetFloat4(elements[i])));
            }
            return;
        case ConverterKind::Float8:
            for (int i = 0; i < nitems; i++) {
                writer.double_(DatumGetFloat8(elements[i]));
            }
            return;
        case ConverterKind::Bool:
            for (int i = 0; i < nitems; i++) {
                writer.boolean(DatumGetBool(elements[i]));
            }
            return;
        case ConverterKind::Text:
        case ConverterKind::JsonText:
            for (int i = 0; i < nitems; i++) {
                msgpack_write_text(writer, elements[i]);
            }
            return;
        case ConverterKind::Uuid:
            for (int i = 0; i < nitems; i++) {
                char out[36];
                format_uuid(elements[i], out);
                writer.string(std::string_view(out, sizeof(out)));
            }
            return;
        case ConverterKind::NameText:
            for (int i = 0; i < nitems; i++) {
                writer.string(name_text_view(elements[i]));
            }
            return;
        case ConverterKind::CharText:
            for (int i = 0; i < nitems; i++) {
                char ch = DatumGetChar(elements[i]);
                writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
            }
            return;
        case ConverterKind::EnumText:
            for (int i = 0; i < nitems; i++) {
                writer.string(enum_label_view(elements[i]));
            }
            return;
        case ConverterKind::InetText:
            for (int i = 0; i < nitems; i++) {
                write_network_string(writer, elements[i], false);
            }
            return;
        case ConverterKind::CidrText:
            for (int i = 0; i < nitems; i++) {
                write_network_string(writer, elements[i], true);
            }
            return;
        case ConverterKind::IntervalText:
            for (int i = 0; i < nitems; i++) {
                write_interval_string(writer, elements[i]);
            }
            return;
        case ConverterKind::Numeric:
            for (int i = 0; i < nitems; i++) {
                numeric_write_fast(writer, elements[i]);
            }
            return;
        case ConverterKind::Date:
            for (int i = 0; i < nitems; i++) {
                writer.int64(static_cast<int64_t>(DatumGetDateADT(elements[i])));
            }
            return;
        case ConverterKind::Timestamp:
            for (int i = 0; i < nitems; i++) {
                writer.int64(static_cast<int64_t>(DatumGetTimestamp(elements[i])));
            }
            return;
        case ConverterKind::Timestamptz:
            for (int i = 0; i < nitems; i++) {
                writer.int64(static_cast<int64_t>(DatumGetTimestampTz(elements[i])));
            }
            return;
        case ConverterKind::Jsonb:
            for (int i = 0; i < nitems; i++) {
                writer.binary(datum_jsonb_span(elements[i]));
            }
            return;
        case ConverterKind::Bytea:
            for (int i = 0; i < nitems; i++) {
                writer.binary(datum_bytea_span(elements[i]));
            }
            return;
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("unsupported array element type for fast MessagePack path")));
    }
}

static inline void msgpack_store_be16(uint8_t* out, uint16_t value)
{
    out[0] = static_cast<uint8_t>(value >> 8);
    out[1] = static_cast<uint8_t>(value);
}

static inline void msgpack_store_be32(uint8_t* out, uint32_t value)
{
    out[0] = static_cast<uint8_t>(value >> 24);
    out[1] = static_cast<uint8_t>(value >> 16);
    out[2] = static_cast<uint8_t>(value >> 8);
    out[3] = static_cast<uint8_t>(value);
}

static inline void msgpack_store_be64(uint8_t* out, uint64_t value)
{
    msgpack_store_be32(out, static_cast<uint32_t>(value >> 32));
    msgpack_store_be32(out + 4, static_cast<uint32_t>(value));
}

static inline uint8_t* msgpack_encode_int64(uint8_t* out, int64_t value)
{
    if (value >= 0) {
        uint64_t unsigned_value = static_cast<uint64_t>(value);
        if (unsigned_value <= 0x7f) {
            *out++ = static_cast<uint8_t>(unsigned_value);
        } else if (unsigned_value <= UINT8_MAX) {
            *out++ = 0xcc;
            *out++ = static_cast<uint8_t>(unsigned_value);
        } else if (unsigned_value <= UINT16_MAX) {
            *out++ = 0xcd;
            msgpack_store_be16(out, static_cast<uint16_t>(unsigned_value));
            out += 2;
        } else if (unsigned_value <= UINT32_MAX) {
            *out++ = 0xce;
            msgpack_store_be32(out, static_cast<uint32_t>(unsigned_value));
            out += 4;
        } else {
            *out++ = 0xcf;
            msgpack_store_be64(out, unsigned_value);
            out += 8;
        }
    } else if (value >= -32) {
        *out++ = static_cast<uint8_t>(value);
    } else if (value >= INT8_MIN) {
        *out++ = 0xd0;
        *out++ = static_cast<uint8_t>(value);
    } else if (value >= INT16_MIN) {
        *out++ = 0xd1;
        msgpack_store_be16(out, static_cast<uint16_t>(value));
        out += 2;
    } else if (value >= INT32_MIN) {
        *out++ = 0xd2;
        msgpack_store_be32(out, static_cast<uint32_t>(value));
        out += 4;
    } else {
        *out++ = 0xd3;
        msgpack_store_be64(out, static_cast<uint64_t>(value));
        out += 8;
    }
    return out;
}

template <typename ValueT>
static inline void msgpack_write_int64_sequence(
    z::MsgPackSerializer& writer, const ValueT* values, int nitems)
{
    uint8_t* begin = writer.reserve_raw_append(static_cast<size_t>(nitems) * 9);
    uint8_t* out = begin;
    for (int i = 0; i < nitems; i++) {
        out = msgpack_encode_int64(out, static_cast<int64_t>(values[i]));
    }
    writer.commit_raw_append(static_cast<size_t>(out - begin));
}

template <typename ValueT>
static inline void msgpack_write_double_sequence(
    z::MsgPackSerializer& writer, const ValueT* values, int nitems)
{
    uint8_t* begin = writer.reserve_raw_append(static_cast<size_t>(nitems) * 9);
    uint8_t* out = begin;
    for (int i = 0; i < nitems; i++) {
        *out++ = 0xcb;
        uint64_t bits = std::bit_cast<uint64_t>(static_cast<double>(values[i]));
        msgpack_store_be64(out, bits);
        out += 8;
    }
    writer.commit_raw_append(static_cast<size_t>(out - begin));
}

static inline void msgpack_write_bool_sequence(
    z::MsgPackSerializer& writer, const bool* values, int nitems)
{
    uint8_t* begin = writer.reserve_raw_append(static_cast<size_t>(nitems));
    for (int i = 0; i < nitems; i++) {
        begin[i] = values[i] ? 0xc3 : 0xc2;
    }
    writer.commit_raw_append(static_cast<size_t>(nitems));
}

static inline bool msgpack_write_fixed_array_no_nulls(
    z::MsgPackSerializer& writer,
    ConverterKind elem_kind,
    ArrayType* arr,
    int nitems)
{
    switch (elem_kind) {
        case ConverterKind::Int2:
        case ConverterKind::Int4:
        case ConverterKind::Int8:
        case ConverterKind::Float4:
        case ConverterKind::Float8:
        case ConverterKind::Bool:
        case ConverterKind::Uuid:
        case ConverterKind::NameText:
        case ConverterKind::CharText:
        case ConverterKind::EnumText:
        case ConverterKind::IntervalText:
        case ConverterKind::Date:
        case ConverterKind::Timestamp:
        case ConverterKind::Timestamptz:
            break;
        default:
            return false;
    }

    const char* data = ARR_DATA_PTR(arr);
    writer.begin_array(static_cast<size_t>(nitems));
    switch (elem_kind) {
        case ConverterKind::Int2:
        {
            const int16* values = reinterpret_cast<const int16*>(data);
            msgpack_write_int64_sequence(writer, values, nitems);
            break;
        }
        case ConverterKind::Int4:
        {
            const int32* values = reinterpret_cast<const int32*>(data);
            msgpack_write_int64_sequence(writer, values, nitems);
            break;
        }
        case ConverterKind::Int8:
        {
            const int64* values = reinterpret_cast<const int64*>(data);
            msgpack_write_int64_sequence(writer, values, nitems);
            break;
        }
        case ConverterKind::Float4:
        {
            const float4* values = reinterpret_cast<const float4*>(data);
            msgpack_write_double_sequence(writer, values, nitems);
            break;
        }
        case ConverterKind::Float8:
        {
            const float8* values = reinterpret_cast<const float8*>(data);
            msgpack_write_double_sequence(writer, values, nitems);
            break;
        }
        case ConverterKind::Bool:
        {
            const bool* values = reinterpret_cast<const bool*>(data);
            msgpack_write_bool_sequence(writer, values, nitems);
            break;
        }
        case ConverterKind::Uuid:
        {
            const pg_uuid_t* values = reinterpret_cast<const pg_uuid_t*>(data);
            for (int i = 0; i < nitems; i++) {
                char out[36];
                format_uuid(PointerGetDatum(&values[i]), out);
                writer.string(std::string_view(out, sizeof(out)));
            }
            break;
        }
        case ConverterKind::NameText:
        {
            const NameData* values = reinterpret_cast<const NameData*>(data);
            for (int i = 0; i < nitems; i++) writer.string(name_text_view(NameGetDatum(&values[i])));
            break;
        }
        case ConverterKind::CharText:
            for (int i = 0; i < nitems; i++) {
                char ch = data[i];
                writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
            }
            break;
        case ConverterKind::EnumText:
        {
            const Oid* values = reinterpret_cast<const Oid*>(data);
            for (int i = 0; i < nitems; i++) writer.string(enum_label_view(ObjectIdGetDatum(values[i])));
            break;
        }
        case ConverterKind::IntervalText:
        {
            const Interval* values = reinterpret_cast<const Interval*>(data);
            for (int i = 0; i < nitems; i++) write_interval_string(writer, PointerGetDatum(&values[i]));
            break;
        }
        case ConverterKind::Date:
        {
            const DateADT* values = reinterpret_cast<const DateADT*>(data);
            msgpack_write_int64_sequence(writer, values, nitems);
            break;
        }
        case ConverterKind::Timestamp:
        case ConverterKind::Timestamptz:
        {
            const int64* values = reinterpret_cast<const int64*>(data);
            msgpack_write_int64_sequence(writer, values, nitems);
            break;
        }
        default:
            pg_unreachable();
    }
    writer.end_array();
    return true;
}

static inline void msgpack_write_array(
    z::MsgPackSerializer& writer,
    const CachedColumn& col,
    Datum value)
{
    ArrayType* arr = DatumGetArrayTypeP(value);
    int ndim = ARR_NDIM(arr);

    if (ndim == 0) {
        writer.begin_array(0);
        writer.end_array();
        return;
    }

    if (ndim != 1) {
        z::dyn::serialize(array_to_dynamic(value, col.typid), writer);
        return;
    }

    int nitems = ArrayGetNItems(ndim, ARR_DIMS(arr));
    if (!ARR_HASNULL(arr) &&
        msgpack_write_fixed_array_no_nulls(writer, col.array_element_kind, arr, nitems)) {
        return;
    }

    Datum* elements;
    bool* nulls;
    deconstruct_array(arr,
                      col.array_element_typid,
                      col.array_typlen,
                      col.array_typbyval,
                      col.array_typalign,
                      &elements,
                      &nulls,
                      &nitems);

    MsgpackArrayElemWriterFn elem_writer = col.msgpack_array_elem_writer;
    if (elem_writer == nullptr) {
        elem_writer = &msgpack_array_elem_unsupported;
    }

    writer.begin_array(static_cast<size_t>(nitems));
    if (col.array_element_kind == ConverterKind::Fallback) {
        for (int i = 0; i < nitems; i++) {
            if (nulls[i]) {
                writer.null();
            } else {
                write_output_string(writer, col.array_element_typoutput, elements[i]);
            }
        }
    } else if (!ARR_HASNULL(arr)) {
        msgpack_write_array_no_nulls(writer, col.array_element_kind, elements, nitems);
    } else {
        for (int i = 0; i < nitems; i++) {
            elem_writer(writer, elements[i], nulls[i]);
        }
    }
    writer.end_array();

    pfree(elements);
    pfree(nulls);
}

static inline void msgpack_scalar_unsupported(
    z::MsgPackSerializer& writer,
    const CachedColumn& col,
    Datum value,
    bool isnull)
{
    (void)writer;
    (void)value;
    (void)isnull;
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("unsupported type for fast MessagePack path"),
             errdetail("column type OID %u", col.typid)));
}

static inline void msgpack_scalar_fallback(
    z::MsgPackSerializer& writer, const CachedColumn& col, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    char* str = OidOutputFunctionCall(col.typoutput, value);
    writer.string(std::string_view(str));
    pfree(str);
}

static inline void msgpack_scalar_int2(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetInt16(value)));
}

static inline void msgpack_scalar_int4(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetInt32(value)));
}

static inline void msgpack_scalar_int8(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetInt64(value)));
}

static inline void msgpack_scalar_float4(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.double_(static_cast<double>(DatumGetFloat4(value)));
}

static inline void msgpack_scalar_float8(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.double_(DatumGetFloat8(value));
}

static inline void msgpack_scalar_bool(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.boolean(DatumGetBool(value));
}

static inline void msgpack_scalar_text(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    msgpack_write_text(writer, value);
}

static inline void msgpack_scalar_numeric(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    numeric_write_fast(writer, value);
}

static inline void msgpack_scalar_date(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetDateADT(value)));
}

static inline void msgpack_scalar_timestamp(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetTimestamp(value)));
}

static inline void msgpack_scalar_timestamptz(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.int64(static_cast<int64_t>(DatumGetTimestampTz(value)));
}

static inline void msgpack_scalar_jsonb(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.binary(datum_jsonb_span(value));
}

static inline void msgpack_scalar_bytea(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.binary(datum_bytea_span(value));
}

static inline void msgpack_scalar_array(
    z::MsgPackSerializer& writer, const CachedColumn& col, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    msgpack_write_array(writer, col, value);
}

static inline void msgpack_scalar_uuid(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    char out[36];
    format_uuid(value, out);
    writer.string(std::string_view(out, sizeof(out)));
}

static inline void msgpack_scalar_name(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.string(name_text_view(value));
}

static inline void msgpack_scalar_char(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    char ch = DatumGetChar(value);
    writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
}

static inline void msgpack_scalar_enum(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    writer.string(enum_label_view(value));
}

static inline void msgpack_scalar_inet(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    write_network_string(writer, value, false);
}

static inline void msgpack_scalar_cidr(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    write_network_string(writer, value, true);
}

static inline void msgpack_scalar_interval(
    z::MsgPackSerializer& writer, const CachedColumn&, Datum value, bool isnull)
{
    if (isnull) { writer.null(); return; }
    write_interval_string(writer, value);
}

static MsgpackScalarWriterFn select_msgpack_scalar_writer(ConverterKind kind)
{
    switch (kind) {
        case ConverterKind::Int2: return &msgpack_scalar_int2;
        case ConverterKind::Int4: return &msgpack_scalar_int4;
        case ConverterKind::Int8: return &msgpack_scalar_int8;
        case ConverterKind::Float4: return &msgpack_scalar_float4;
        case ConverterKind::Float8: return &msgpack_scalar_float8;
        case ConverterKind::Bool: return &msgpack_scalar_bool;
        case ConverterKind::Text:
        case ConverterKind::JsonText:
            return &msgpack_scalar_text;
        case ConverterKind::Uuid: return &msgpack_scalar_uuid;
        case ConverterKind::NameText: return &msgpack_scalar_name;
        case ConverterKind::CharText: return &msgpack_scalar_char;
        case ConverterKind::EnumText: return &msgpack_scalar_enum;
        case ConverterKind::InetText: return &msgpack_scalar_inet;
        case ConverterKind::CidrText: return &msgpack_scalar_cidr;
        case ConverterKind::IntervalText: return &msgpack_scalar_interval;
        case ConverterKind::Numeric: return &msgpack_scalar_numeric;
        case ConverterKind::Date: return &msgpack_scalar_date;
        case ConverterKind::Timestamp: return &msgpack_scalar_timestamp;
        case ConverterKind::Timestamptz: return &msgpack_scalar_timestamptz;
        case ConverterKind::Jsonb: return &msgpack_scalar_jsonb;
        case ConverterKind::Bytea: return &msgpack_scalar_bytea;
        case ConverterKind::Array: return &msgpack_scalar_array;
        case ConverterKind::Fallback: return &msgpack_scalar_fallback;
        case ConverterKind::Composite: return &msgpack_scalar_unsupported;
    }
    return &msgpack_scalar_unsupported;
}

static inline void msgpack_write_record_map(
    z::MsgPackSerializer& writer,
    HeapTupleHeader rec,
    const CachedSchema& schema,
    TupleDeformScratch* scratch)
{
    HeapTupleData tuple;
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    tuple.t_data = rec;

    if (schema.columns.size() > 15) {
        writer.begin_map_preencoded(schema.msgpack_map_header_ptr, schema.msgpack_map_header_len);
    } else {
        writer.begin_map(schema.columns.size());
    }
    if (!schema.use_deform_access) {
        for (const CachedColumn& col : schema.columns) {
            bool isnull;
            Datum value = heap_getattr(&tuple, col.attnum, schema.tupdesc, &isnull);
            writer.key_preencoded(col.msgpack_key_ptr, col.msgpack_key_len);
            col.msgpack_scalar_writer(writer, col, value, isnull);
        }
    } else {
        const size_t nattrs = static_cast<size_t>(schema.tupdesc->natts);
        scratch->ensure(nattrs);
        heap_deform_tuple(&tuple, schema.tupdesc, scratch->values, scratch->nulls);
        for (const CachedColumn& col : schema.columns) {
            const int idx = col.attnum - 1;
            writer.key_preencoded(col.msgpack_key_ptr, col.msgpack_key_len);
            col.msgpack_scalar_writer(writer, col, scratch->values[idx], scratch->nulls[idx]);
        }
    }
    writer.end_map();
}

static inline z::MsgPackRootSerializer& msgpack_reusable_root()
{
    static z::MsgPackRootSerializer rs;
    return rs;
}

static inline bytea* msgpack_result_from_reusable_root(z::MsgPackRootSerializer& rs)
{
    size_t len = rs.sbuf.size;
    bytea* result = (bytea*) palloc(len + VARHDRSZ);
    SET_VARSIZE(result, len + VARHDRSZ);
    if (len > 0) {
        memcpy(VARDATA(result), rs.sbuf.data, len);
    }
    msgpack_sbuffer_clear(&rs.sbuf);
    return result;
}

static bytea* try_serialize_msgpack_row_fast(HeapTupleHeader rec)
{
    Oid tupType = HeapTupleHeaderGetTypeId(rec);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
    const CachedSchema& schema = get_cached_schema(tupType, tupTypmod);

    if (!schema.msgpack_fast_supported) {
        return nullptr;
    }

    z::MsgPackRootSerializer& rs = msgpack_reusable_root();
    msgpack_sbuffer_clear(&rs.sbuf);

    try {
        z::MsgPackSerializer writer(rs);
        if (!schema.use_deform_access) {
            msgpack_write_record_map(writer, rec, schema, nullptr);
        } else {
            TupleDeformScratch scratch;
            msgpack_write_record_map(writer, rec, schema, &scratch);
        }

        return msgpack_result_from_reusable_root(rs);
    } catch (const std::exception& ex) {
        msgpack_sbuffer_clear(&rs.sbuf);
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast MessagePack row serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        msgpack_sbuffer_clear(&rs.sbuf);
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast MessagePack row serialization failed with unknown exception")));
    }

    return nullptr;
}

static bytea* try_serialize_msgpack_array_fast(Datum* elements, bool* nulls, int nitems)
{
    std::vector<const CachedSchema*> schemas;
    schemas.reserve(nitems);

    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            schemas.push_back(nullptr);
            continue;
        }

        HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
        Oid tupType = HeapTupleHeaderGetTypeId(rec);
        int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
        const CachedSchema& schema = get_cached_schema(tupType, tupTypmod);

        if (!schema.msgpack_fast_supported) {
            return nullptr;
        }
        schemas.push_back(&schema);
    }

    z::MsgPackRootSerializer& rs = msgpack_reusable_root();
    msgpack_sbuffer_clear(&rs.sbuf);

    try {
        z::MsgPackSerializer writer(rs);
        TupleDeformScratch scratch;

        writer.begin_array(static_cast<size_t>(nitems));
        for (int i = 0; i < nitems; i++) {
            if (nulls[i]) {
                writer.null();
            } else {
                HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
                msgpack_write_record_map(writer, rec, *schemas[i], &scratch);
            }
        }
        writer.end_array();

        return msgpack_result_from_reusable_root(rs);
    } catch (const std::exception& ex) {
        msgpack_sbuffer_clear(&rs.sbuf);
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast MessagePack batch serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        msgpack_sbuffer_clear(&rs.sbuf);
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast MessagePack batch serialization failed with unknown exception")));
    }

    return nullptr;
}

static inline void cbor_write_text(z::cborjc::Serializer& writer, Datum value)
{
    text* txt = DatumGetTextPP(value);
    const char* ptr = VARDATA_ANY(txt);
    int len = VARSIZE_ANY_EXHDR(txt);
    writer.string(std::string_view(ptr, static_cast<size_t>(len)));
}

static inline void cbor_write_array_element(
    z::cborjc::Serializer& writer,
    ConverterKind elem_kind,
    Datum value,
    bool isnull)
{
    if (isnull) {
        writer.null();
        return;
    }

    switch (elem_kind) {
        case ConverterKind::Int2:
            writer.int64(static_cast<int64_t>(DatumGetInt16(value)));
            return;
        case ConverterKind::Int4:
            writer.int64(static_cast<int64_t>(DatumGetInt32(value)));
            return;
        case ConverterKind::Int8:
            writer.int64(static_cast<int64_t>(DatumGetInt64(value)));
            return;
        case ConverterKind::Float4:
            writer.double_(static_cast<double>(DatumGetFloat4(value)));
            return;
        case ConverterKind::Float8:
            writer.double_(DatumGetFloat8(value));
            return;
        case ConverterKind::Bool:
            writer.boolean(DatumGetBool(value));
            return;
        case ConverterKind::Text:
        case ConverterKind::JsonText:
            cbor_write_text(writer, value);
            return;
        case ConverterKind::Uuid:
        {
            char out[36];
            format_uuid(value, out);
            writer.string(std::string_view(out, sizeof(out)));
            return;
        }
        case ConverterKind::NameText:
            writer.string(name_text_view(value));
            return;
        case ConverterKind::CharText:
        {
            char ch = DatumGetChar(value);
            writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
            return;
        }
        case ConverterKind::EnumText:
            writer.string(enum_label_view(value));
            return;
        case ConverterKind::InetText:
            write_network_string(writer, value, false);
            return;
        case ConverterKind::CidrText:
            write_network_string(writer, value, true);
            return;
        case ConverterKind::IntervalText:
            write_interval_string(writer, value);
            return;
        case ConverterKind::Numeric:
            numeric_write_fast(writer, value);
            return;
        case ConverterKind::Date:
            writer.int64(static_cast<int64_t>(DatumGetDateADT(value)));
            return;
        case ConverterKind::Timestamp:
            writer.int64(static_cast<int64_t>(DatumGetTimestamp(value)));
            return;
        case ConverterKind::Timestamptz:
            writer.int64(static_cast<int64_t>(DatumGetTimestampTz(value)));
            return;
        case ConverterKind::Jsonb:
            writer.binary(datum_jsonb_span(value));
            return;
        case ConverterKind::Bytea:
            writer.binary(datum_bytea_span(value));
            return;
        default:
            break;
    }

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("unsupported array element type for fast CBOR path")));
}

static inline void cbor_write_array(
    z::cborjc::Serializer& writer,
    const CachedColumn& col,
    Datum value)
{
    ArrayType* arr = DatumGetArrayTypeP(value);
    int ndim = ARR_NDIM(arr);

    if (ndim == 0) {
        writer.begin_array(0);
        writer.end_array();
        return;
    }

    if (ndim != 1) {
        z::dyn::serialize(array_to_dynamic(value, col.typid), writer);
        return;
    }

    Datum* elements;
    bool* nulls;
    int nitems;
    deconstruct_array(arr,
                      col.array_element_typid,
                      col.array_typlen,
                      col.array_typbyval,
                      col.array_typalign,
                      &elements,
                      &nulls,
                      &nitems);

    writer.begin_array(static_cast<size_t>(nitems));
    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            writer.null();
        } else if (col.array_element_kind == ConverterKind::Fallback) {
            write_output_string(writer, col.array_element_typoutput, elements[i]);
        } else {
            cbor_write_array_element(writer, col.array_element_kind, elements[i], false);
        }
    }
    writer.end_array();

    pfree(elements);
    pfree(nulls);
}

static inline void cbor_write_scalar(
    z::cborjc::Serializer& writer,
    const CachedColumn& col,
    Datum value,
    bool isnull)
{
    if (isnull) {
        writer.null();
        return;
    }

    switch (col.kind) {
        case ConverterKind::Int2:
            writer.int64(static_cast<int64_t>(DatumGetInt16(value)));
            return;
        case ConverterKind::Int4:
            writer.int64(static_cast<int64_t>(DatumGetInt32(value)));
            return;
        case ConverterKind::Int8:
            writer.int64(static_cast<int64_t>(DatumGetInt64(value)));
            return;
        case ConverterKind::Float4:
            writer.double_(static_cast<double>(DatumGetFloat4(value)));
            return;
        case ConverterKind::Float8:
            writer.double_(DatumGetFloat8(value));
            return;
        case ConverterKind::Bool:
            writer.boolean(DatumGetBool(value));
            return;
        case ConverterKind::Text:
        case ConverterKind::JsonText:
            cbor_write_text(writer, value);
            return;
        case ConverterKind::Uuid:
        {
            char out[36];
            format_uuid(value, out);
            writer.string(std::string_view(out, sizeof(out)));
            return;
        }
        case ConverterKind::NameText:
            writer.string(name_text_view(value));
            return;
        case ConverterKind::CharText:
        {
            char ch = DatumGetChar(value);
            writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
            return;
        }
        case ConverterKind::EnumText:
            writer.string(enum_label_view(value));
            return;
        case ConverterKind::InetText:
            write_network_string(writer, value, false);
            return;
        case ConverterKind::CidrText:
            write_network_string(writer, value, true);
            return;
        case ConverterKind::IntervalText:
            write_interval_string(writer, value);
            return;
        case ConverterKind::Numeric:
            numeric_write_fast(writer, value);
            return;
        case ConverterKind::Date:
            writer.int64(static_cast<int64_t>(DatumGetDateADT(value)));
            return;
        case ConverterKind::Timestamp:
            writer.int64(static_cast<int64_t>(DatumGetTimestamp(value)));
            return;
        case ConverterKind::Timestamptz:
            writer.int64(static_cast<int64_t>(DatumGetTimestampTz(value)));
            return;
        case ConverterKind::Jsonb:
            writer.binary(datum_jsonb_span(value));
            return;
        case ConverterKind::Bytea:
            writer.binary(datum_bytea_span(value));
            return;
        case ConverterKind::Array:
            cbor_write_array(writer, col, value);
            return;
        case ConverterKind::Fallback:
        {
            char* str = OidOutputFunctionCall(col.typoutput, value);
            writer.string(std::string_view(str));
            pfree(str);
            return;
        }
        default:
            break;
    }

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("unsupported type for fast CBOR path"),
             errdetail("column type OID %u", col.typid)));
}

static inline void cbor_write_record_map(
    z::cborjc::Serializer& writer,
    HeapTupleHeader rec,
    const CachedSchema& schema,
    TupleDeformScratch* scratch)
{
    HeapTupleData tuple;
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    tuple.t_data = rec;

    writer.begin_map(schema.columns.size());
    if (!schema.use_deform_access) {
        for (const CachedColumn& col : schema.columns) {
            bool isnull;
            Datum value = heap_getattr(&tuple, col.attnum, schema.tupdesc, &isnull);
            writer.key(col.name);
            cbor_write_scalar(writer, col, value, isnull);
        }
    } else {
        const size_t nattrs = static_cast<size_t>(schema.tupdesc->natts);
        scratch->ensure(nattrs);
        heap_deform_tuple(&tuple, schema.tupdesc, scratch->values, scratch->nulls);
        for (const CachedColumn& col : schema.columns) {
            const int idx = col.attnum - 1;
            writer.key(col.name);
            cbor_write_scalar(writer, col, scratch->values[idx], scratch->nulls[idx]);
        }
    }
    writer.end_map();
}

static bytea* try_serialize_cbor_row_fast(HeapTupleHeader rec)
{
    Oid tupType = HeapTupleHeaderGetTypeId(rec);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
    const CachedSchema& schema = get_cached_schema(tupType, tupTypmod);

    if (!schema.cbor_fast_supported) {
        return nullptr;
    }

    try {
        z::cborjc::RootSerializer rs;
        z::cborjc::Serializer writer(rs);
        if (!schema.use_deform_access) {
            cbor_write_record_map(writer, rec, schema, nullptr);
        } else {
            TupleDeformScratch scratch;
            cbor_write_record_map(writer, rec, schema, &scratch);
        }

        z::ZBuffer buffer = rs.finish();
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast CBOR row serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast CBOR row serialization failed with unknown exception")));
    }

    return nullptr;
}

static bytea* try_serialize_cbor_array_fast(Datum* elements, bool* nulls, int nitems)
{
    std::vector<const CachedSchema*> schemas;
    schemas.reserve(nitems);

    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            schemas.push_back(nullptr);
            continue;
        }

        HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
        Oid tupType = HeapTupleHeaderGetTypeId(rec);
        int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
        const CachedSchema& schema = get_cached_schema(tupType, tupTypmod);

        if (!schema.cbor_fast_supported) {
            return nullptr;
        }
        schemas.push_back(&schema);
    }

    try {
        z::cborjc::RootSerializer rs;
        z::cborjc::Serializer writer(rs);
        TupleDeformScratch scratch;

        writer.begin_array(static_cast<size_t>(nitems));
        for (int i = 0; i < nitems; i++) {
            if (nulls[i]) {
                writer.null();
            } else {
                HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
                cbor_write_record_map(writer, rec, *schemas[i], &scratch);
            }
        }
        writer.end_array();

        z::ZBuffer buffer = rs.finish();
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast CBOR batch serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast CBOR batch serialization failed with unknown exception")));
    }

    return nullptr;
}

static inline void zera_write_text(z::zera::Serializer& writer, Datum value)
{
    text* txt = DatumGetTextPP(value);
    const char* ptr = VARDATA_ANY(txt);
    int len = VARSIZE_ANY_EXHDR(txt);
    writer.string(std::string_view(ptr, static_cast<size_t>(len)));
}

static inline void zera_write_array_element(
    z::zera::Serializer& writer,
    ConverterKind elem_kind,
    Datum value,
    bool isnull)
{
    if (isnull) {
        writer.null();
        return;
    }

    switch (elem_kind) {
        case ConverterKind::Int2:
            writer.int64(static_cast<int64_t>(DatumGetInt16(value)));
            return;
        case ConverterKind::Int4:
            writer.int64(static_cast<int64_t>(DatumGetInt32(value)));
            return;
        case ConverterKind::Int8:
            writer.int64(static_cast<int64_t>(DatumGetInt64(value)));
            return;
        case ConverterKind::Float4:
            writer.double_(static_cast<double>(DatumGetFloat4(value)));
            return;
        case ConverterKind::Float8:
            writer.double_(DatumGetFloat8(value));
            return;
        case ConverterKind::Bool:
            writer.boolean(DatumGetBool(value));
            return;
        case ConverterKind::Text:
        case ConverterKind::JsonText:
            zera_write_text(writer, value);
            return;
        case ConverterKind::Uuid:
        {
            char out[36];
            format_uuid(value, out);
            writer.string(std::string_view(out, sizeof(out)));
            return;
        }
        case ConverterKind::NameText:
            writer.string(name_text_view(value));
            return;
        case ConverterKind::CharText:
        {
            char ch = DatumGetChar(value);
            writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
            return;
        }
        case ConverterKind::EnumText:
            writer.string(enum_label_view(value));
            return;
        case ConverterKind::InetText:
            write_network_string(writer, value, false);
            return;
        case ConverterKind::CidrText:
            write_network_string(writer, value, true);
            return;
        case ConverterKind::IntervalText:
            write_interval_string(writer, value);
            return;
        case ConverterKind::Numeric:
            numeric_write_fast(writer, value);
            return;
        case ConverterKind::Date:
            writer.int64(static_cast<int64_t>(DatumGetDateADT(value)));
            return;
        case ConverterKind::Timestamp:
            writer.int64(static_cast<int64_t>(DatumGetTimestamp(value)));
            return;
        case ConverterKind::Timestamptz:
            writer.int64(static_cast<int64_t>(DatumGetTimestampTz(value)));
            return;
        case ConverterKind::Jsonb:
            writer.binary(datum_jsonb_span(value));
            return;
        case ConverterKind::Bytea:
            writer.binary(datum_bytea_span(value));
            return;
        default:
            break;
    }

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("unsupported array element type for fast ZERA path")));
}

static inline void zera_write_array(
    z::zera::Serializer& writer,
    const CachedColumn& col,
    Datum value)
{
    ArrayType* arr = DatumGetArrayTypeP(value);
    int ndim = ARR_NDIM(arr);

    if (ndim == 0) {
        writer.begin_array(0);
        writer.end_array();
        return;
    }

    if (ndim != 1) {
        z::dyn::serialize(array_to_dynamic(value, col.typid), writer);
        return;
    }

    Datum* elements;
    bool* nulls;
    int nitems;
    deconstruct_array(arr,
                      col.array_element_typid,
                      col.array_typlen,
                      col.array_typbyval,
                      col.array_typalign,
                      &elements,
                      &nulls,
                      &nitems);

    writer.begin_array(static_cast<size_t>(nitems));
    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            writer.null();
        } else if (col.array_element_kind == ConverterKind::Fallback) {
            write_output_string(writer, col.array_element_typoutput, elements[i]);
        } else {
            zera_write_array_element(writer, col.array_element_kind, elements[i], false);
        }
    }
    writer.end_array();

    pfree(elements);
    pfree(nulls);
}

static inline void zera_write_scalar(
    z::zera::Serializer& writer,
    const CachedColumn& col,
    Datum value,
    bool isnull)
{
    if (isnull) {
        writer.null();
        return;
    }

    switch (col.kind) {
        case ConverterKind::Int2:
            writer.int64(static_cast<int64_t>(DatumGetInt16(value)));
            return;
        case ConverterKind::Int4:
            writer.int64(static_cast<int64_t>(DatumGetInt32(value)));
            return;
        case ConverterKind::Int8:
            writer.int64(static_cast<int64_t>(DatumGetInt64(value)));
            return;
        case ConverterKind::Float4:
            writer.double_(static_cast<double>(DatumGetFloat4(value)));
            return;
        case ConverterKind::Float8:
            writer.double_(DatumGetFloat8(value));
            return;
        case ConverterKind::Bool:
            writer.boolean(DatumGetBool(value));
            return;
        case ConverterKind::Text:
        case ConverterKind::JsonText:
            zera_write_text(writer, value);
            return;
        case ConverterKind::Uuid:
        {
            char out[36];
            format_uuid(value, out);
            writer.string(std::string_view(out, sizeof(out)));
            return;
        }
        case ConverterKind::NameText:
            writer.string(name_text_view(value));
            return;
        case ConverterKind::CharText:
        {
            char ch = DatumGetChar(value);
            writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
            return;
        }
        case ConverterKind::EnumText:
            writer.string(enum_label_view(value));
            return;
        case ConverterKind::InetText:
            write_network_string(writer, value, false);
            return;
        case ConverterKind::CidrText:
            write_network_string(writer, value, true);
            return;
        case ConverterKind::IntervalText:
            write_interval_string(writer, value);
            return;
        case ConverterKind::Numeric:
            numeric_write_fast(writer, value);
            return;
        case ConverterKind::Date:
            writer.int64(static_cast<int64_t>(DatumGetDateADT(value)));
            return;
        case ConverterKind::Timestamp:
            writer.int64(static_cast<int64_t>(DatumGetTimestamp(value)));
            return;
        case ConverterKind::Timestamptz:
            writer.int64(static_cast<int64_t>(DatumGetTimestampTz(value)));
            return;
        case ConverterKind::Jsonb:
            writer.binary(datum_jsonb_span(value));
            return;
        case ConverterKind::Bytea:
            writer.binary(datum_bytea_span(value));
            return;
        case ConverterKind::Array:
            zera_write_array(writer, col, value);
            return;
        case ConverterKind::Fallback:
        {
            char* str = OidOutputFunctionCall(col.typoutput, value);
            writer.string(std::string_view(str));
            pfree(str);
            return;
        }
        default:
            break;
    }

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("unsupported type for fast ZERA path"),
             errdetail("column type OID %u", col.typid)));
}

static inline void zera_write_record_map(
    z::zera::Serializer& writer,
    HeapTupleHeader rec,
    const CachedSchema& schema,
    TupleDeformScratch* scratch)
{
    HeapTupleData tuple;
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    tuple.t_data = rec;

    writer.begin_map(schema.columns.size());
    if (!schema.use_deform_access) {
        for (const CachedColumn& col : schema.columns) {
            bool isnull;
            Datum value = heap_getattr(&tuple, col.attnum, schema.tupdesc, &isnull);
            writer.key_preencoded(col.zera_key_encoded);
            zera_write_scalar(writer, col, value, isnull);
        }
    } else {
        const size_t nattrs = static_cast<size_t>(schema.tupdesc->natts);
        scratch->ensure(nattrs);
        heap_deform_tuple(&tuple, schema.tupdesc, scratch->values, scratch->nulls);
        for (const CachedColumn& col : schema.columns) {
            const int idx = col.attnum - 1;
            writer.key_preencoded(col.zera_key_encoded);
            zera_write_scalar(writer, col, scratch->values[idx], scratch->nulls[idx]);
        }
    }
    writer.end_map();
}

static bytea* try_serialize_zera_row_fast(HeapTupleHeader rec)
{
    Oid tupType = HeapTupleHeaderGetTypeId(rec);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
    const CachedSchema& schema = get_cached_schema(tupType, tupTypmod);

    if (!schema.zera_fast_supported) {
        return nullptr;
    }

    try {
        z::zera::RootSerializer rs;
        z::zera::Serializer writer(rs);
        if (!schema.use_deform_access) {
            zera_write_record_map(writer, rec, schema, nullptr);
        } else {
            TupleDeformScratch scratch;
            zera_write_record_map(writer, rec, schema, &scratch);
        }

        z::ZBuffer buffer = rs.finish();
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast ZERA row serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast ZERA row serialization failed with unknown exception")));
    }

    return nullptr;
}

static bytea* try_serialize_zera_array_fast(Datum* elements, bool* nulls, int nitems)
{
    std::vector<const CachedSchema*> schemas;
    schemas.reserve(nitems);

    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            schemas.push_back(nullptr);
            continue;
        }

        HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
        Oid tupType = HeapTupleHeaderGetTypeId(rec);
        int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
        const CachedSchema& schema = get_cached_schema(tupType, tupTypmod);

        if (!schema.zera_fast_supported) {
            return nullptr;
        }
        schemas.push_back(&schema);
    }

    try {
        z::zera::RootSerializer rs;
        z::zera::Serializer writer(rs);
        TupleDeformScratch scratch;

        writer.begin_array(static_cast<size_t>(nitems));
        for (int i = 0; i < nitems; i++) {
            if (nulls[i]) {
                writer.null();
            } else {
                HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
                zera_write_record_map(writer, rec, *schemas[i], &scratch);
            }
        }
        writer.end_array();

        z::ZBuffer buffer = rs.finish();
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast ZERA batch serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast ZERA batch serialization failed with unknown exception")));
    }

    return nullptr;
}

static inline void flex_write_text(z::flex::Serializer& writer, Datum value)
{
    text* txt = DatumGetTextPP(value);
    const char* ptr = VARDATA_ANY(txt);
    int len = VARSIZE_ANY_EXHDR(txt);
    writer.string(std::string_view(ptr, static_cast<size_t>(len)));
}

static inline void flex_write_array_element(
    z::flex::Serializer& writer,
    ConverterKind elem_kind,
    Datum value,
    bool isnull)
{
    if (isnull) {
        writer.null();
        return;
    }

    switch (elem_kind) {
        case ConverterKind::Int2:
            writer.int64(static_cast<int64_t>(DatumGetInt16(value)));
            return;
        case ConverterKind::Int4:
            writer.int64(static_cast<int64_t>(DatumGetInt32(value)));
            return;
        case ConverterKind::Int8:
            writer.int64(static_cast<int64_t>(DatumGetInt64(value)));
            return;
        case ConverterKind::Float4:
            writer.double_(static_cast<double>(DatumGetFloat4(value)));
            return;
        case ConverterKind::Float8:
            writer.double_(DatumGetFloat8(value));
            return;
        case ConverterKind::Bool:
            writer.boolean(DatumGetBool(value));
            return;
        case ConverterKind::Text:
        case ConverterKind::JsonText:
            flex_write_text(writer, value);
            return;
        case ConverterKind::Uuid:
        {
            char out[36];
            format_uuid(value, out);
            writer.string(std::string_view(out, sizeof(out)));
            return;
        }
        case ConverterKind::NameText:
            writer.string(name_text_view(value));
            return;
        case ConverterKind::CharText:
        {
            char ch = DatumGetChar(value);
            writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
            return;
        }
        case ConverterKind::EnumText:
            writer.string(enum_label_view(value));
            return;
        case ConverterKind::InetText:
            write_network_string(writer, value, false);
            return;
        case ConverterKind::CidrText:
            write_network_string(writer, value, true);
            return;
        case ConverterKind::IntervalText:
            write_interval_string(writer, value);
            return;
        case ConverterKind::Numeric:
            numeric_write_fast(writer, value);
            return;
        case ConverterKind::Date:
            writer.int64(static_cast<int64_t>(DatumGetDateADT(value)));
            return;
        case ConverterKind::Timestamp:
            writer.int64(static_cast<int64_t>(DatumGetTimestamp(value)));
            return;
        case ConverterKind::Timestamptz:
            writer.int64(static_cast<int64_t>(DatumGetTimestampTz(value)));
            return;
        case ConverterKind::Jsonb:
            writer.binary(datum_jsonb_span(value));
            return;
        case ConverterKind::Bytea:
            writer.binary(datum_bytea_span(value));
            return;
        default:
            break;
    }

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("unsupported array element type for fast Flex path")));
}

static inline void flex_write_array(
    z::flex::Serializer& writer,
    const CachedColumn& col,
    Datum value)
{
    ArrayType* arr = DatumGetArrayTypeP(value);
    int ndim = ARR_NDIM(arr);

    if (ndim == 0) {
        writer.begin_array(0);
        writer.end_array();
        return;
    }

    if (ndim != 1) {
        z::dyn::serialize(array_to_dynamic(value, col.typid), writer);
        return;
    }

    Datum* elements;
    bool* nulls;
    int nitems;
    deconstruct_array(arr,
                      col.array_element_typid,
                      col.array_typlen,
                      col.array_typbyval,
                      col.array_typalign,
                      &elements,
                      &nulls,
                      &nitems);

    writer.begin_array(static_cast<size_t>(nitems));
    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            writer.null();
        } else if (col.array_element_kind == ConverterKind::Fallback) {
            write_output_string(writer, col.array_element_typoutput, elements[i]);
        } else {
            flex_write_array_element(writer, col.array_element_kind, elements[i], false);
        }
    }
    writer.end_array();

    pfree(elements);
    pfree(nulls);
}

static inline void flex_write_scalar(
    z::flex::Serializer& writer,
    const CachedColumn& col,
    Datum value,
    bool isnull)
{
    if (isnull) {
        writer.null();
        return;
    }

    switch (col.kind) {
        case ConverterKind::Int2:
            writer.int64(static_cast<int64_t>(DatumGetInt16(value)));
            return;
        case ConverterKind::Int4:
            writer.int64(static_cast<int64_t>(DatumGetInt32(value)));
            return;
        case ConverterKind::Int8:
            writer.int64(static_cast<int64_t>(DatumGetInt64(value)));
            return;
        case ConverterKind::Float4:
            writer.double_(static_cast<double>(DatumGetFloat4(value)));
            return;
        case ConverterKind::Float8:
            writer.double_(DatumGetFloat8(value));
            return;
        case ConverterKind::Bool:
            writer.boolean(DatumGetBool(value));
            return;
        case ConverterKind::Text:
        case ConverterKind::JsonText:
            flex_write_text(writer, value);
            return;
        case ConverterKind::Uuid:
        {
            char out[36];
            format_uuid(value, out);
            writer.string(std::string_view(out, sizeof(out)));
            return;
        }
        case ConverterKind::NameText:
            writer.string(name_text_view(value));
            return;
        case ConverterKind::CharText:
        {
            char ch = DatumGetChar(value);
            writer.string(std::string_view(&ch, ch == '\0' ? 0 : 1));
            return;
        }
        case ConverterKind::EnumText:
            writer.string(enum_label_view(value));
            return;
        case ConverterKind::InetText:
            write_network_string(writer, value, false);
            return;
        case ConverterKind::CidrText:
            write_network_string(writer, value, true);
            return;
        case ConverterKind::IntervalText:
            write_interval_string(writer, value);
            return;
        case ConverterKind::Numeric:
            numeric_write_fast(writer, value);
            return;
        case ConverterKind::Date:
            writer.int64(static_cast<int64_t>(DatumGetDateADT(value)));
            return;
        case ConverterKind::Timestamp:
            writer.int64(static_cast<int64_t>(DatumGetTimestamp(value)));
            return;
        case ConverterKind::Timestamptz:
            writer.int64(static_cast<int64_t>(DatumGetTimestampTz(value)));
            return;
        case ConverterKind::Jsonb:
            writer.binary(datum_jsonb_span(value));
            return;
        case ConverterKind::Bytea:
            writer.binary(datum_bytea_span(value));
            return;
        case ConverterKind::Array:
            flex_write_array(writer, col, value);
            return;
        case ConverterKind::Fallback:
        {
            char* str = OidOutputFunctionCall(col.typoutput, value);
            writer.string(std::string_view(str));
            pfree(str);
            return;
        }
        default:
            break;
    }

    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("unsupported type for fast Flex path"),
             errdetail("column type OID %u", col.typid)));
}

static inline void flex_write_record_map(
    z::flex::Serializer& writer,
    HeapTupleHeader rec,
    const CachedSchema& schema,
    TupleDeformScratch* scratch)
{
    HeapTupleData tuple;
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    tuple.t_data = rec;

    writer.begin_map(schema.columns.size());
    if (!schema.use_deform_access) {
        for (const CachedColumn& col : schema.columns) {
            bool isnull;
            Datum value = heap_getattr(&tuple, col.attnum, schema.tupdesc, &isnull);
            writer.key(col.name);
            flex_write_scalar(writer, col, value, isnull);
        }
    } else {
        const size_t nattrs = static_cast<size_t>(schema.tupdesc->natts);
        scratch->ensure(nattrs);
        heap_deform_tuple(&tuple, schema.tupdesc, scratch->values, scratch->nulls);
        for (const CachedColumn& col : schema.columns) {
            const int idx = col.attnum - 1;
            writer.key(col.name);
            flex_write_scalar(writer, col, scratch->values[idx], scratch->nulls[idx]);
        }
    }
    writer.end_map();
}

static bytea* try_serialize_flex_row_fast(HeapTupleHeader rec)
{
    Oid tupType = HeapTupleHeaderGetTypeId(rec);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
    const CachedSchema& schema = get_cached_schema(tupType, tupTypmod);

    if (!schema.flex_fast_supported) {
        return nullptr;
    }

    try {
        z::flex::RootSerializer rs;
        z::flex::Serializer writer(rs);
        if (!schema.use_deform_access) {
            flex_write_record_map(writer, rec, schema, nullptr);
        } else {
            TupleDeformScratch scratch;
            flex_write_record_map(writer, rec, schema, &scratch);
        }

        z::ZBuffer buffer = rs.finish();
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast Flex row serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast Flex row serialization failed with unknown exception")));
    }

    return nullptr;
}

static bytea* try_serialize_flex_array_fast(Datum* elements, bool* nulls, int nitems)
{
    std::vector<const CachedSchema*> schemas;
    schemas.reserve(nitems);

    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            schemas.push_back(nullptr);
            continue;
        }

        HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
        Oid tupType = HeapTupleHeaderGetTypeId(rec);
        int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
        const CachedSchema& schema = get_cached_schema(tupType, tupTypmod);

        if (!schema.flex_fast_supported) {
            return nullptr;
        }
        schemas.push_back(&schema);
    }

    try {
        z::flex::RootSerializer rs;
        z::flex::Serializer writer(rs);
        TupleDeformScratch scratch;

        writer.begin_array(static_cast<size_t>(nitems));
        for (int i = 0; i < nitems; i++) {
            if (nulls[i]) {
                writer.null();
            } else {
                HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
                flex_write_record_map(writer, rec, *schemas[i], &scratch);
            }
        }
        writer.end_array();

        z::ZBuffer buffer = rs.finish();
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast Flex batch serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("fast Flex batch serialization failed with unknown exception")));
    }

    return nullptr;
}

/*
 * Convert a PostgreSQL record (HeapTupleHeader) to a zerialize dynamic map
 * This is used by both single-record and batch processing functions
 */
static z::dyn::Value record_to_dynamic_map(HeapTupleHeader rec)
{
    Oid tupType;
    int32 tupTypmod;
    const CachedSchema* schema;
    HeapTupleData tuple;

    // Extract type info from the record
    tupType = HeapTupleHeaderGetTypeId(rec);
    tupTypmod = HeapTupleHeaderGetTypMod(rec);
    schema = &get_cached_schema(tupType, tupTypmod);
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    tuple.t_data = rec;

    // Build map of column name -> value
    // Pre-allocate space to avoid reallocation overhead
    z::dyn::Value::Map entries;
    entries.reserve(schema->columns.size());

    if (!schema->use_deform_access) {
        for (const CachedColumn& col : schema->columns) {
            bool isnull;
            Datum value = heap_getattr(&tuple, col.attnum, schema->tupdesc, &isnull);
            entries.emplace_back(col.name, datum_to_dynamic_cached(value, isnull, col));
        }
    } else {
        TupleDeformScratch scratch;
        const size_t nattrs = static_cast<size_t>(schema->tupdesc->natts);
        scratch.ensure(nattrs);
        heap_deform_tuple(&tuple, schema->tupdesc, scratch.values, scratch.nulls);
        for (const CachedColumn& col : schema->columns) {
            const int idx = col.attnum - 1;
            entries.emplace_back(col.name,
                                 datum_to_dynamic_cached(scratch.values[idx], scratch.nulls[idx], col));
        }
    }

    return z::dyn::Value::map(std::move(entries));
}

/*
 * Generic helper function to convert PostgreSQL tuple to any binary format
 * Template parameter determines the serialization protocol
 */
template<typename Protocol>
static bytea* tuple_to_binary(HeapTupleHeader rec)
{
    try {
        if constexpr (std::is_same_v<Protocol, z::MsgPack>) {
            if (bytea* fast = try_serialize_msgpack_row_fast(rec)) {
                return fast;
            }
        } else if constexpr (std::is_same_v<Protocol, z::CBOR>) {
            if (bytea* fast = try_serialize_cbor_row_fast(rec)) {
                return fast;
            }
        } else if constexpr (std::is_same_v<Protocol, z::Zera>) {
            if (bytea* fast = try_serialize_zera_row_fast(rec)) {
                return fast;
            }
        } else if constexpr (std::is_same_v<Protocol, z::Flex>) {
            if (bytea* fast = try_serialize_flex_row_fast(rec)) {
                return fast;
            }
        }

        // Convert record to dynamic map
        z::dyn::Value map = record_to_dynamic_map(rec);

        // Serialize
        z::ZBuffer buffer = z::serialize<Protocol>(map);
        std::span<const uint8_t> data = buffer.buf();

        // Allocate PostgreSQL bytea with actual size
        // (Estimation helps zerialize internally, allocation uses actual size)
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);

        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("serialization failed with unknown exception")));
    }

    return nullptr;
}

template<typename Protocol>
static bytea* tuple_to_binary_slow(HeapTupleHeader rec)
{
    try {
        z::dyn::Value map = record_to_dynamic_map(rec);
        z::ZBuffer buffer = z::serialize<Protocol>(map);
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("slow-path serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("slow-path serialization failed with unknown exception")));
    }
    return nullptr;
}

static bytea* dynamic_to_msgpack_binary(const z::dyn::Value& v)
{
    try {
        z::ZBuffer buffer = z::serialize<z::MsgPack>(v);
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("msgpack serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("msgpack serialization failed with unknown exception")));
    }
    return nullptr;
}

/*
 * Generic helper function to convert array of PostgreSQL tuples to binary format
 * This is the batch processing version for improved performance
 * Template parameter determines the serialization protocol
 */
template<typename Protocol>
static bytea* array_to_binary(ArrayType* arr)
{
    // Get array element type (should be a record type)
    Oid element_type = ARR_ELEMTYPE(arr);
    int ndim = ARR_NDIM(arr);

    // Handle empty arrays
    if (ndim == 0 || ArrayGetNItems(ndim, ARR_DIMS(arr)) == 0) {
        // Return empty array
        z::ZBuffer buffer = z::serialize<Protocol>(z::dyn::Value::array(z::dyn::Value::Array()));
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    }

    // For now, handle 1D arrays (covers most use cases)
    if (ndim > 1) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("multidimensional arrays not supported for batch serialization")));
    }

    // Batch functions only support arrays of records/composite values.
    if (element_type != RECORDOID && get_typtype(element_type) != TYPTYPE_COMPOSITE) {
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("batch serialization requires an array of composite records"),
                 errdetail("Got array element type OID %u.", element_type)));
    }

    // Get array element info
    int16 typlen;
    bool typbyval;
    char typalign;
    get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

    // Deconstruct array into elements
    Datum* elements;
    bool* nulls;
    int nitems;

    deconstruct_array(arr, element_type, typlen, typbyval, typalign,
                     &elements, &nulls, &nitems);

    if constexpr (std::is_same_v<Protocol, z::MsgPack>) {
        if (bytea* fast = try_serialize_msgpack_array_fast(elements, nulls, nitems)) {
            pfree(elements);
            pfree(nulls);
            return fast;
        }
    } else if constexpr (std::is_same_v<Protocol, z::CBOR>) {
        if (bytea* fast = try_serialize_cbor_array_fast(elements, nulls, nitems)) {
            pfree(elements);
            pfree(nulls);
            return fast;
        }
    } else if constexpr (std::is_same_v<Protocol, z::Zera>) {
        if (bytea* fast = try_serialize_zera_array_fast(elements, nulls, nitems)) {
            pfree(elements);
            pfree(nulls);
            return fast;
        }
    } else if constexpr (std::is_same_v<Protocol, z::Flex>) {
        if (bytea* fast = try_serialize_flex_array_fast(elements, nulls, nitems)) {
            pfree(elements);
            pfree(nulls);
            return fast;
        }
    }

    // Build array of record maps with pre-allocated capacity
    z::dyn::Value::Array result_array;
    result_array.reserve(nitems);  // Pre-allocate array capacity

    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            // Add null for NULL records
            result_array.push_back(z::dyn::Value());
        } else {
            // Convert record to map
            HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
            result_array.push_back(record_to_dynamic_map(rec));
        }
    }

    pfree(elements);
    pfree(nulls);

    try {
        // Serialize the array
        z::ZBuffer buffer = z::serialize<Protocol>(z::dyn::Value::array(std::move(result_array)));
        std::span<const uint8_t> data = buffer.buf();

        // Copy to PostgreSQL bytea
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);

        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("batch serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("batch serialization failed with unknown exception")));
    }

    return nullptr;
}

template<typename Protocol>
static bytea* array_to_binary_slow(ArrayType* arr)
{
    Oid element_type = ARR_ELEMTYPE(arr);
    int ndim = ARR_NDIM(arr);

    if (ndim == 0 || ArrayGetNItems(ndim, ARR_DIMS(arr)) == 0) {
        z::ZBuffer buffer = z::serialize<Protocol>(z::dyn::Value::array(z::dyn::Value::Array()));
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    }

    if (ndim > 1) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("multidimensional arrays not supported for batch serialization")));
    }

    if (element_type != RECORDOID && get_typtype(element_type) != TYPTYPE_COMPOSITE) {
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("batch serialization requires an array of composite records"),
                 errdetail("Got array element type OID %u.", element_type)));
    }

    int16 typlen;
    bool typbyval;
    char typalign;
    get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

    Datum* elements;
    bool* nulls;
    int nitems;
    deconstruct_array(arr, element_type, typlen, typbyval, typalign, &elements, &nulls, &nitems);

    z::dyn::Value::Array result_array;
    result_array.reserve(nitems);

    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            result_array.push_back(z::dyn::Value());
        } else {
            HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
            result_array.push_back(record_to_dynamic_map(rec));
        }
    }

    pfree(elements);
    pfree(nulls);

    try {
        z::ZBuffer buffer = z::serialize<Protocol>(z::dyn::Value::array(std::move(result_array)));
        std::span<const uint8_t> data = buffer.buf();
        size_t len = data.size();
        bytea* result = (bytea*) palloc(len + VARHDRSZ);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), data.data(), len);
        return result;
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("slow-path batch serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("slow-path batch serialization failed with unknown exception")));
    }

    return nullptr;
}

/*
 * Single record serialization functions
 */

/*
 * row_to_flexbuffers - Convert PostgreSQL record to FlexBuffers binary format
 */
extern "C" Datum
row_to_flexbuffers(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec;
    bytea* result;

    // Get the record argument
    rec = PG_GETARG_HEAPTUPLEHEADER(0);

    // Convert to FlexBuffers
    result = tuple_to_binary<z::Flex>(rec);

    PG_RETURN_BYTEA_P(result);
}

/*
 * row_to_msgpack - Convert PostgreSQL record to MessagePack binary format
 */
extern "C" Datum
row_to_msgpack(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec;
    bytea* result;

    // Get the record argument
    rec = PG_GETARG_HEAPTUPLEHEADER(0);

    // Convert to MessagePack
    result = tuple_to_binary<z::MsgPack>(rec);

    PG_RETURN_BYTEA_P(result);
}

/*
 * row_to_msgpack_slow - Convert PostgreSQL record to MessagePack via generic dynamic path
 * Useful for parity/correctness testing against fast path.
 */
extern "C" Datum
row_to_msgpack_slow(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec;
    bytea* result;

    rec = PG_GETARG_HEAPTUPLEHEADER(0);
    result = tuple_to_binary_slow<z::MsgPack>(rec);
    PG_RETURN_BYTEA_P(result);
}

/*
 * msgpack_from_jsonb - Convert nested jsonb to nested MessagePack.
 * This is the core bridge for SQL builder parity workflows.
 */
extern "C" Datum
msgpack_from_jsonb(PG_FUNCTION_ARGS)
{
    Jsonb* jb = PG_GETARG_JSONB_P(0);
    z::dyn::Value v = jsonb_to_dynamic(jb);
    bytea* result = dynamic_to_msgpack_binary(v);
    PG_RETURN_BYTEA_P(result);
}

/*
 * msgpack_to_jsonb - Safely decode one complete MessagePack value to jsonb.
 */
extern "C" Datum
msgpack_to_jsonb(PG_FUNCTION_ARGS)
{
    bytea* input = PG_GETARG_BYTEA_PP(0);
    const auto* bytes = reinterpret_cast<const uint8_t*>(VARDATA_ANY(input));
    const size_t length = static_cast<size_t>(VARSIZE_ANY_EXHDR(input));
    std::span<const uint8_t> data(bytes, length);

    try {
        const size_t consumed = msgpack_validate_value(data, 0);
        if (consumed != data.size()) {
            throw z::DeserializationError("trailing bytes after MessagePack value");
        }

        z::MsgPackDeserializer reader(data);
        std::string json;
        json.reserve(length + 32);
        msgpack_reader_to_json(reader, json);
        PG_FREE_IF_COPY(input, 0);

        Datum result = DirectFunctionCall1(jsonb_in, CStringGetDatum(json.c_str()));
        PG_RETURN_DATUM(result);
    } catch (const std::exception& ex) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("invalid MessagePack input"),
                 errdetail("%s", ex.what())));
    } catch (...) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("invalid MessagePack input"),
                 errdetail("unknown decoding error")));
    }

    PG_RETURN_NULL();
}

/*
 * msgpack_build_object - Build a MessagePack object from variadic key/value args.
 * Mirrors json_build_object semantics at SQL layer.
 */
extern "C" Datum
msgpack_build_object(PG_FUNCTION_ARGS)
{
    const int nargs = PG_NARGS();
    if ((nargs % 2) != 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("msgpack_build_object requires an even number of arguments")));
    }

    z::dyn::Value::Map entries;
    entries.reserve(static_cast<size_t>(nargs / 2));

    for (int i = 0; i < nargs; i += 2) {
        if (PG_ARGISNULL(i)) {
            ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                     errmsg("msgpack_build_object key must not be null")));
        }

        Oid key_typid = get_fn_expr_argtype(fcinfo->flinfo, i);
        if (!OidIsValid(key_typid)) {
            key_typid = TEXTOID;
        }
        std::string key = datum_to_string_output(PG_GETARG_DATUM(i), key_typid);

        Oid val_typid = get_fn_expr_argtype(fcinfo->flinfo, i + 1);
        if (!OidIsValid(val_typid)) {
            val_typid = TEXTOID;
        }
        Datum val = (i + 1 < nargs && !PG_ARGISNULL(i + 1)) ? PG_GETARG_DATUM(i + 1) : (Datum) 0;
        entries.emplace_back(std::move(key), datum_to_dynamic(val, val_typid, PG_ARGISNULL(i + 1)));
    }

    bytea* result = dynamic_to_msgpack_binary(z::dyn::Value::map(std::move(entries)));
    PG_RETURN_BYTEA_P(result);
}

/*
 * msgpack_build_array - Build a MessagePack array from variadic arguments.
 * Mirrors json_build_array semantics at SQL layer.
 */
extern "C" Datum
msgpack_build_array(PG_FUNCTION_ARGS)
{
    const int nargs = PG_NARGS();
    z::dyn::Value::Array arr;
    arr.reserve(static_cast<size_t>(nargs));

    for (int i = 0; i < nargs; i++) {
        Oid typid = get_fn_expr_argtype(fcinfo->flinfo, i);
        if (!OidIsValid(typid)) {
            typid = TEXTOID;
        }
        Datum val = (!PG_ARGISNULL(i)) ? PG_GETARG_DATUM(i) : (Datum) 0;
        arr.push_back(datum_to_dynamic(val, typid, PG_ARGISNULL(i)));
    }

    bytea* result = dynamic_to_msgpack_binary(z::dyn::Value::array(std::move(arr)));
    PG_RETURN_BYTEA_P(result);
}

/*
 * msgpack_agg_final - Finalize jsonb_agg state and convert to MessagePack.
 */
extern "C" Datum
msgpack_agg_final(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    Datum agg = DirectFunctionCall1(jsonb_agg_finalfn, PG_GETARG_DATUM(0));
    if (DatumGetPointer(agg) == nullptr) {
        PG_RETURN_NULL();
    }
    Jsonb* jb = DatumGetJsonbP(agg);
    z::dyn::Value v = jsonb_to_dynamic(jb);
    bytea* result = dynamic_to_msgpack_binary(v);
    PG_RETURN_BYTEA_P(result);
}

/*
 * msgpack_object_agg_final - Finalize jsonb_object_agg state and convert.
 */
extern "C" Datum
msgpack_object_agg_final(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    Datum agg = DirectFunctionCall1(jsonb_object_agg_finalfn, PG_GETARG_DATUM(0));
    if (DatumGetPointer(agg) == nullptr) {
        PG_RETURN_NULL();
    }
    Jsonb* jb = DatumGetJsonbP(agg);
    z::dyn::Value v = jsonb_to_dynamic(jb);
    bytea* result = dynamic_to_msgpack_binary(v);
    PG_RETURN_BYTEA_P(result);
}

/*
 * row_to_cbor - Convert PostgreSQL record to CBOR binary format
 */
extern "C" Datum
row_to_cbor(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec;
    bytea* result;

    // Get the record argument
    rec = PG_GETARG_HEAPTUPLEHEADER(0);

    // Convert to CBOR
    result = tuple_to_binary<z::CBOR>(rec);

    PG_RETURN_BYTEA_P(result);
}

/*
 * row_to_zera - Convert PostgreSQL record to ZERA binary format
 */
extern "C" Datum
row_to_zera(PG_FUNCTION_ARGS)
{
    HeapTupleHeader rec;
    bytea* result;

    // Get the record argument
    rec = PG_GETARG_HEAPTUPLEHEADER(0);

    // Convert to ZERA
    result = tuple_to_binary<z::Zera>(rec);

    PG_RETURN_BYTEA_P(result);
}

/*
 * Batch processing functions (multiple records at once)
 */

/*
 * rows_to_flexbuffers - Convert array of PostgreSQL records to FlexBuffers binary format
 */
extern "C" Datum
rows_to_flexbuffers(PG_FUNCTION_ARGS)
{
    ArrayType* arr;
    bytea* result;

    // Get the array argument
    arr = PG_GETARG_ARRAYTYPE_P(0);

    // Convert to FlexBuffers array
    result = array_to_binary<z::Flex>(arr);

    PG_RETURN_BYTEA_P(result);
}

/*
 * rows_to_msgpack - Convert array of PostgreSQL records to MessagePack binary format
 */
extern "C" Datum
rows_to_msgpack(PG_FUNCTION_ARGS)
{
    ArrayType* arr;
    bytea* result;

    // Get the array argument
    arr = PG_GETARG_ARRAYTYPE_P(0);

    // Convert to MessagePack array
    result = array_to_binary<z::MsgPack>(arr);

    PG_RETURN_BYTEA_P(result);
}

/*
 * rows_to_msgpack_slow - Convert array of records via generic dynamic path
 * Useful for parity/correctness testing against fast path.
 */
extern "C" Datum
rows_to_msgpack_slow(PG_FUNCTION_ARGS)
{
    ArrayType* arr;
    bytea* result;

    arr = PG_GETARG_ARRAYTYPE_P(0);
    result = array_to_binary_slow<z::MsgPack>(arr);
    PG_RETURN_BYTEA_P(result);
}

/*
 * rows_to_cbor - Convert array of PostgreSQL records to CBOR binary format
 */
extern "C" Datum
rows_to_cbor(PG_FUNCTION_ARGS)
{
    ArrayType* arr;
    bytea* result;

    // Get the array argument
    arr = PG_GETARG_ARRAYTYPE_P(0);

    // Convert to CBOR array
    result = array_to_binary<z::CBOR>(arr);

    PG_RETURN_BYTEA_P(result);
}

/*
 * rows_to_zera - Convert array of PostgreSQL records to ZERA binary format
 */
extern "C" Datum
rows_to_zera(PG_FUNCTION_ARGS)
{
    ArrayType* arr;
    bytea* result;

    // Get the array argument
    arr = PG_GETARG_ARRAYTYPE_P(0);

    // Convert to ZERA array
    result = array_to_binary<z::Zera>(arr);

    PG_RETURN_BYTEA_P(result);
}
