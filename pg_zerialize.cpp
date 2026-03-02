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
#include <type_traits>
#include <cmath>
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
    Datum row_to_msgpack_slow(PG_FUNCTION_ARGS);
    Datum msgpack_from_jsonb(PG_FUNCTION_ARGS);
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
        col.array_element_kind = ConverterKind::Fallback;
        col.array_typlen = 0;
        col.array_typbyval = false;
        col.array_typalign = 'i';

        if (col.kind == ConverterKind::Array) {
            col.array_element_typid = get_element_type(col.typid);
            if (OidIsValid(col.array_element_typid)) {
                col.array_element_kind = classify_type(col.array_element_typid);
                col.msgpack_array_elem_writer = select_msgpack_array_elem_writer(col.array_element_kind);
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

static inline bool numeric_try_int64(Datum value, int64_t* out)
{
    Numeric num = DatumGetNumeric(value);
    bool have_error = false;
    int64 v = numeric_int8_opt_error(num, &have_error);
    if (!have_error) {
        *out = static_cast<int64_t>(v);
        return true;
    }
    return false;
}

static inline double numeric_to_float8_fast(Datum value)
{
    Datum float_val = DirectFunctionCall1(numeric_float8, value);
    return DatumGetFloat8(float_val);
}

template <typename WriterT>
static inline void numeric_write_fast(WriterT& writer, Datum value)
{
    int64_t i64;
    if (numeric_try_int64(value, &i64)) {
        writer.int64(i64);
    } else {
        writer.double_(numeric_to_float8_fast(value));
    }
}

static inline z::dyn::Value numeric_to_dynamic_fast(Datum value)
{
    int64_t i64;
    if (numeric_try_int64(value, &i64)) {
        return z::dyn::Value(i64);
    }
    return z::dyn::Value(numeric_to_float8_fast(value));
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
        {
            text* txt = DatumGetTextP(value);
            char* str = text_to_cstring(txt);
            std::string strval(str);
            pfree(str);
            return z::dyn::Value(strval);
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
        case ConverterKind::Text: return &msgpack_array_elem_text;
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
            for (int i = 0; i < nitems; i++) {
                msgpack_write_text(writer, elements[i]);
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

    MsgpackArrayElemWriterFn elem_writer = col.msgpack_array_elem_writer;
    if (elem_writer == nullptr) {
        elem_writer = &msgpack_array_elem_unsupported;
    }

    writer.begin_array(static_cast<size_t>(nitems));
    if (!ARR_HASNULL(arr)) {
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
            cbor_write_text(writer, value);
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
