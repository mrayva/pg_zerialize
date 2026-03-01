/*
 * pg_zerialize.cpp
 * PostgreSQL extension for converting rows to binary formats using zerialize
 * Starting with FlexBuffers support
 */

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"
#include "access/tupdesc.h"
#include "executor/spi.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/inval.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
}

#include <vector>
#include <string>
#include <string_view>
#include <cstring>
#include <array>
#include <exception>
#include <memory>
#include <unordered_map>
#include <type_traits>
#include <zerialize/zerialize.hpp>
#include <zerialize/protocols/flex.hpp>
#include <zerialize/protocols/msgpack.hpp>
#include <zerialize/protocols/cbor.hpp>
#include <zerialize/protocols/zera.hpp>
#include <zerialize/dynamic.hpp>

namespace z = zerialize;

/*
 * Forward declarations
 */
extern "C" {
    void _PG_init(void);

    // Single record functions
    Datum row_to_flexbuffers(PG_FUNCTION_ARGS);
    Datum row_to_msgpack(PG_FUNCTION_ARGS);
    Datum row_to_cbor(PG_FUNCTION_ARGS);
    Datum row_to_zera(PG_FUNCTION_ARGS);

    // Batch processing functions
    Datum rows_to_flexbuffers(PG_FUNCTION_ARGS);
    Datum rows_to_msgpack(PG_FUNCTION_ARGS);
    Datum rows_to_cbor(PG_FUNCTION_ARGS);
    Datum rows_to_zera(PG_FUNCTION_ARGS);

    PG_FUNCTION_INFO_V1(row_to_flexbuffers);
    PG_FUNCTION_INFO_V1(row_to_msgpack);
    PG_FUNCTION_INFO_V1(row_to_cbor);
    PG_FUNCTION_INFO_V1(row_to_zera);

    PG_FUNCTION_INFO_V1(rows_to_flexbuffers);
    PG_FUNCTION_INFO_V1(rows_to_msgpack);
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
    Numeric,
    Date,
    Timestamp,
    Timestamptz,
    Jsonb,
    Bytea,
    Array,
    Fallback
};

struct CachedColumn;
using MsgpackScalarWriterFn = void (*)(z::MsgPackSerializer&, const CachedColumn&, Datum, bool);

struct CachedColumn {
    int attnum;
    std::string name;
    std::vector<uint8_t> msgpack_key_encoded;
    std::vector<uint8_t> zera_key_encoded;
    MsgpackScalarWriterFn msgpack_scalar_writer;
    Oid typid;
    Oid typoutput;
    ConverterKind kind;
    Oid array_element_typid;
    ConverterKind array_element_kind;
    int16 array_typlen;
    bool array_typbyval;
    char array_typalign;
};

struct CachedSchema {
    TupleDesc tupdesc;
    std::vector<CachedColumn> columns;
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
static std::vector<uint8_t> encode_zera_key(std::string_view key);
static MsgpackScalarWriterFn select_msgpack_scalar_writer(ConverterKind kind);
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

static inline void clear_tupdesc_cache()
{
    schema_cache.clear();
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
    CacheRegisterSyscacheCallback(TYPEOID, tupdesc_syscache_callback, (Datum) 0);
    CacheRegisterSyscacheCallback(RELOID, tupdesc_syscache_callback, (Datum) 0);
    CacheRegisterSyscacheCallback(ATTNUM, tupdesc_syscache_callback, (Datum) 0);
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
    TupleDesc blessed = BlessTupleDesc(tupdesc);
    ReleaseTupleDesc(tupdesc);

    CachedSchema schema;
    schema.tupdesc = blessed;
    schema.use_deform_access = false;
    schema.msgpack_fast_supported = true;
    schema.cbor_fast_supported = true;
    schema.zera_fast_supported = true;
    schema.flex_fast_supported = true;
    schema.columns.reserve(blessed->natts);

    for (int i = 0; i < blessed->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(blessed, i);
        if (att->attisdropped) {
            continue;
        }

        CachedColumn col;
        col.attnum = i + 1;
        col.name = std::string(NameStr(att->attname));
        col.msgpack_key_encoded = encode_msgpack_string_key(col.name);
        col.zera_key_encoded = encode_zera_key(col.name);
        col.msgpack_scalar_writer = nullptr;
        col.typid = att->atttypid;
        col.kind = classify_type(col.typid);
        col.msgpack_scalar_writer = select_msgpack_scalar_writer(col.kind);
        col.typoutput = InvalidOid;
        col.array_element_typid = InvalidOid;
        col.array_element_kind = ConverterKind::Fallback;
        col.array_typlen = 0;
        col.array_typbyval = false;
        col.array_typalign = 'i';

        if (col.kind == ConverterKind::Array) {
            col.array_element_typid = get_element_type(col.typid);
            if (OidIsValid(col.array_element_typid)) {
                col.array_element_kind = classify_type(col.array_element_typid);
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
    }

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

/*
 * Convert a PostgreSQL array to a zerialize dyn::Value array
 */
static z::dyn::Value array_to_dynamic(Datum value, Oid typid)
{
    ArrayType* arr = DatumGetArrayTypeP(value);
    int ndim = ARR_NDIM(arr);

    // Handle empty arrays
    if (ndim == 0 || ArrayGetNItems(ndim, ARR_DIMS(arr)) == 0) {
        return z::dyn::Value::array(z::dyn::Value::Array());
    }

    // For now, handle 1D arrays (covers 90% of use cases)
    // Multi-dimensional arrays could be added later
    if (ndim > 1) {
        // Fall back to text representation for multi-dimensional arrays
        Oid typoutput;
        bool typIsVarlena;
        char* str;

        getTypeOutputInfo(typid, &typoutput, &typIsVarlena);
        str = OidOutputFunctionCall(typoutput, value);
        std::string strval(str);
        pfree(str);
        return z::dyn::Value(strval);
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

    // Build dyn::Value::Array
    z::dyn::Value::Array result_array;
    result_array.reserve(nitems);

    for (int i = 0; i < nitems; i++) {
        result_array.push_back(datum_to_dynamic(elements[i], element_type, nulls[i]));
    }

    pfree(elements);
    pfree(nulls);

    return z::dyn::Value::array(std::move(result_array));
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
        {
            text* txt = DatumGetTextP(value);
            char* str = text_to_cstring(txt);
            std::string strval(str);
            pfree(str);
            return z::dyn::Value(strval);
        }

        case NUMERICOID:
        {
            // Convert NUMERIC to double (float8)
            // This works for most use cases (prices, percentages, measurements)
            // Note: May lose precision for very large or very precise decimals
            Datum float_val = DirectFunctionCall1(numeric_float8, value);
            return z::dyn::Value(DatumGetFloat8(float_val));
        }
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

        // TODO: Add support for more types:
        // - JSONOID (nested JSON)
        // - Composite types (nested records)

        default:
        {
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
        {
            text* txt = DatumGetTextP(value);
            char* str = text_to_cstring(txt);
            std::string strval(str);
            pfree(str);
            return z::dyn::Value(strval);
        }
        case ConverterKind::Numeric:
        {
            Datum float_val = DirectFunctionCall1(numeric_float8, value);
            return z::dyn::Value(DatumGetFloat8(float_val));
        }
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
        case ConverterKind::Numeric:
        case ConverterKind::Date:
        case ConverterKind::Timestamp:
        case ConverterKind::Timestamptz:
        case ConverterKind::Jsonb:
        case ConverterKind::Bytea:
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
        case ConverterKind::Numeric:
        case ConverterKind::Date:
        case ConverterKind::Timestamp:
        case ConverterKind::Timestamptz:
        case ConverterKind::Jsonb:
        case ConverterKind::Bytea:
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

static inline void msgpack_write_array_element(
    z::MsgPackSerializer& writer,
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
            msgpack_write_text(writer, value);
            return;
        case ConverterKind::Numeric:
        {
            Datum float_val = DirectFunctionCall1(numeric_float8, value);
            writer.double_(DatumGetFloat8(float_val));
            return;
        }
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
             errmsg("unsupported array element type for fast MessagePack path")));
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
        // Preserve existing behavior: multidimensional arrays fall back to text.
        char* str = OidOutputFunctionCall(col.typoutput, value);
        writer.string(std::string_view(str));
        pfree(str);
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
        msgpack_write_array_element(writer, col.array_element_kind, elements[i], nulls[i]);
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
    Datum float_val = DirectFunctionCall1(numeric_float8, value);
    writer.double_(DatumGetFloat8(float_val));
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

static MsgpackScalarWriterFn select_msgpack_scalar_writer(ConverterKind kind)
{
    switch (kind) {
        case ConverterKind::Int2: return &msgpack_scalar_int2;
        case ConverterKind::Int4: return &msgpack_scalar_int4;
        case ConverterKind::Int8: return &msgpack_scalar_int8;
        case ConverterKind::Float4: return &msgpack_scalar_float4;
        case ConverterKind::Float8: return &msgpack_scalar_float8;
        case ConverterKind::Bool: return &msgpack_scalar_bool;
        case ConverterKind::Text: return &msgpack_scalar_text;
        case ConverterKind::Numeric: return &msgpack_scalar_numeric;
        case ConverterKind::Date: return &msgpack_scalar_date;
        case ConverterKind::Timestamp: return &msgpack_scalar_timestamp;
        case ConverterKind::Timestamptz: return &msgpack_scalar_timestamptz;
        case ConverterKind::Jsonb: return &msgpack_scalar_jsonb;
        case ConverterKind::Bytea: return &msgpack_scalar_bytea;
        case ConverterKind::Array: return &msgpack_scalar_array;
        case ConverterKind::Fallback: return &msgpack_scalar_unsupported;
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

    writer.begin_map(schema.columns.size());
    if (!schema.use_deform_access) {
        for (const CachedColumn& col : schema.columns) {
            bool isnull;
            Datum value = heap_getattr(&tuple, col.attnum, schema.tupdesc, &isnull);
            writer.key_preencoded(col.msgpack_key_encoded);
            col.msgpack_scalar_writer(writer, col, value, isnull);
        }
    } else {
        const size_t nattrs = static_cast<size_t>(schema.tupdesc->natts);
        scratch->ensure(nattrs);
        heap_deform_tuple(&tuple, schema.tupdesc, scratch->values, scratch->nulls);
        for (const CachedColumn& col : schema.columns) {
            const int idx = col.attnum - 1;
            writer.key_preencoded(col.msgpack_key_encoded);
            col.msgpack_scalar_writer(writer, col, scratch->values[idx], scratch->nulls[idx]);
        }
    }
    writer.end_map();
}

static bytea* try_serialize_msgpack_row_fast(HeapTupleHeader rec)
{
    Oid tupType = HeapTupleHeaderGetTypeId(rec);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
    const CachedSchema& schema = get_cached_schema(tupType, tupTypmod);

    if (!schema.msgpack_fast_supported) {
        return nullptr;
    }

    try {
        z::MsgPackRootSerializer rs;
        z::MsgPackSerializer writer(rs);
        if (!schema.use_deform_access) {
            msgpack_write_record_map(writer, rec, schema, nullptr);
        } else {
            TupleDeformScratch scratch;
            msgpack_write_record_map(writer, rec, schema, &scratch);
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
                 errmsg("fast MessagePack row serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
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

    try {
        z::MsgPackRootSerializer rs;
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
                 errmsg("fast MessagePack batch serialization failed"),
                 errdetail("%s", ex.what())));
    } catch (...) {
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
            cbor_write_text(writer, value);
            return;
        case ConverterKind::Numeric:
        {
            Datum float_val = DirectFunctionCall1(numeric_float8, value);
            writer.double_(DatumGetFloat8(float_val));
            return;
        }
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
        // Preserve existing behavior: multidimensional arrays fall back to text.
        char* str = OidOutputFunctionCall(col.typoutput, value);
        writer.string(std::string_view(str));
        pfree(str);
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
        cbor_write_array_element(writer, col.array_element_kind, elements[i], nulls[i]);
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
            cbor_write_text(writer, value);
            return;
        case ConverterKind::Numeric:
        {
            Datum float_val = DirectFunctionCall1(numeric_float8, value);
            writer.double_(DatumGetFloat8(float_val));
            return;
        }
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
            zera_write_text(writer, value);
            return;
        case ConverterKind::Numeric:
        {
            Datum float_val = DirectFunctionCall1(numeric_float8, value);
            writer.double_(DatumGetFloat8(float_val));
            return;
        }
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
        // Preserve existing behavior: multidimensional arrays fall back to text.
        char* str = OidOutputFunctionCall(col.typoutput, value);
        writer.string(std::string_view(str));
        pfree(str);
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
        zera_write_array_element(writer, col.array_element_kind, elements[i], nulls[i]);
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
            zera_write_text(writer, value);
            return;
        case ConverterKind::Numeric:
        {
            Datum float_val = DirectFunctionCall1(numeric_float8, value);
            writer.double_(DatumGetFloat8(float_val));
            return;
        }
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
            flex_write_text(writer, value);
            return;
        case ConverterKind::Numeric:
        {
            Datum float_val = DirectFunctionCall1(numeric_float8, value);
            writer.double_(DatumGetFloat8(float_val));
            return;
        }
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
        // Preserve existing behavior: multidimensional arrays fall back to text.
        char* str = OidOutputFunctionCall(col.typoutput, value);
        writer.string(std::string_view(str));
        pfree(str);
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
        flex_write_array_element(writer, col.array_element_kind, elements[i], nulls[i]);
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
            flex_write_text(writer, value);
            return;
        case ConverterKind::Numeric:
        {
            Datum float_val = DirectFunctionCall1(numeric_float8, value);
            writer.double_(DatumGetFloat8(float_val));
            return;
        }
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
