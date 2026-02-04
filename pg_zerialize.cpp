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
#include "access/tupdesc.h"
#include "executor/spi.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
}

#include <vector>
#include <string>
#include <cstring>
#include <unordered_map>
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

// Global cache for TupleDesc lookups
static std::unordered_map<TypeCacheKey, TupleDesc> tupdesc_cache;

/*
 * Get TupleDesc with caching to avoid repeated system catalog queries
 */
static TupleDesc get_cached_tupdesc(Oid tupType, int32 tupTypmod)
{
    TypeCacheKey key{tupType, tupTypmod};

    auto it = tupdesc_cache.find(key);
    if (it != tupdesc_cache.end()) {
        return it->second;
    }

    // Not in cache, look it up
    TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    // Make it permanent so we can cache it (no need to release later)
    TupleDesc blessed = BlessTupleDesc(tupdesc);

    // Cache the blessed descriptor
    tupdesc_cache[key] = blessed;

    // Release the original (we're keeping the blessed one)
    ReleaseTupleDesc(tupdesc);

    return blessed;
}

/*
 * Forward declaration for recursive array handling
 */
static z::dyn::Value datum_to_dynamic(Datum value, Oid typid, bool isnull);

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
    char typtype = get_typtype(typid);
    if (typtype == TYPTYPE_BASE || typtype == TYPTYPE_RANGE ||
        typtype == TYPTYPE_ENUM || typtype == TYPTYPE_COMPOSITE) {
        // Check if the type name ends with [] or is marked as an array
        Oid array_element_type = get_element_type(typid);
        if (OidIsValid(array_element_type)) {
            // This is an array type
            return array_to_dynamic(value, typid);
        }
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

        // TODO: Add support for more types:
        // - DATEOID, TIMESTAMPOID (dates/timestamps)
        // - JSONOID, JSONBOID (nested JSON)
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

/*
 * Convert a PostgreSQL record (HeapTupleHeader) to a zerialize dynamic map
 * This is used by both single-record and batch processing functions
 */
static z::dyn::Value record_to_dynamic_map(HeapTupleHeader rec)
{
    Oid tupType;
    int32 tupTypmod;
    TupleDesc tupdesc;
    HeapTupleData tuple;
    int ncolumns;

    // Extract type info from the record
    tupType = HeapTupleHeaderGetTypeId(rec);
    tupTypmod = HeapTupleHeaderGetTypMod(rec);
    tupdesc = get_cached_tupdesc(tupType, tupTypmod);

    // Build a temporary HeapTuple for attribute access
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    tuple.t_data = rec;

    ncolumns = tupdesc->natts;

    // Build map of column name -> value
    z::dyn::Value::Map entries;

    for (int i = 0; i < ncolumns; i++) {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        Datum value;
        bool isnull;

        // Skip dropped columns
        if (att->attisdropped)
            continue;

        // Get the attribute value
        value = heap_getattr(&tuple, i + 1, tupdesc, &isnull);

        // Get column name
        const char* attname = NameStr(att->attname);

        // Add to map
        entries.emplace_back(std::string(attname), datum_to_dynamic(value, att->atttypid, isnull));
    }

    // No need to release tupdesc - it's cached and blessed (permanent)

    return z::dyn::Value::map(std::move(entries));
}

/*
 * Generic helper function to convert PostgreSQL tuple to any binary format
 * Template parameter determines the serialization protocol
 */
template<typename Protocol>
static bytea* tuple_to_binary(HeapTupleHeader rec)
{
    // Convert record to dynamic map
    z::dyn::Value map = record_to_dynamic_map(rec);

    // Serialize
    z::ZBuffer buffer = z::serialize<Protocol>(map);
    std::span<const uint8_t> data = buffer.buf();

    // Copy to PostgreSQL bytea
    size_t len = data.size();
    bytea* result = (bytea*) palloc(len + VARHDRSZ);
    SET_VARSIZE(result, len + VARHDRSZ);
    memcpy(VARDATA(result), data.data(), len);

    return result;
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

    // Build array of record maps
    z::dyn::Value::Array result_array;
    result_array.reserve(nitems);

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

    // Serialize the array
    z::ZBuffer buffer = z::serialize<Protocol>(z::dyn::Value::array(std::move(result_array)));
    std::span<const uint8_t> data = buffer.buf();

    // Copy to PostgreSQL bytea
    size_t len = data.size();
    bytea* result = (bytea*) palloc(len + VARHDRSZ);
    SET_VARSIZE(result, len + VARHDRSZ);
    memcpy(VARDATA(result), data.data(), len);

    return result;
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
