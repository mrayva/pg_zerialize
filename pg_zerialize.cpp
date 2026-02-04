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
    Datum row_to_flexbuffers(PG_FUNCTION_ARGS);
    Datum row_to_msgpack(PG_FUNCTION_ARGS);
    Datum row_to_cbor(PG_FUNCTION_ARGS);
    Datum row_to_zera(PG_FUNCTION_ARGS);

    PG_FUNCTION_INFO_V1(row_to_flexbuffers);
    PG_FUNCTION_INFO_V1(row_to_msgpack);
    PG_FUNCTION_INFO_V1(row_to_cbor);
    PG_FUNCTION_INFO_V1(row_to_zera);
}

/*
 * Helper class to dynamically build a serialized map from PostgreSQL tuple
 * Using zerialize's dyn::Value API for dynamic data
 * Template parameter allows using different protocols (Flex, MsgPack, etc.)
 */
template<typename Protocol>
class SerializationBuilder {
private:
    z::dyn::Value::Map entries;

public:
    void add(const std::string& key, z::dyn::Value value) {
        entries.emplace_back(key, std::move(value));
    }

    z::ZBuffer build() {
        // Build a dynamic map using zerialize's dyn::Value
        return z::serialize<Protocol>(z::dyn::Value::map(std::move(entries)));
    }
};

// Type aliases for specific protocols
using FlexBuffersBuilder = SerializationBuilder<z::Flex>;
using MessagePackBuilder = SerializationBuilder<z::MsgPack>;
using CBORBuilder = SerializationBuilder<z::CBOR>;
using ZERABuilder = SerializationBuilder<z::Zera>;

/*
 * Convert a PostgreSQL Datum to a zerialize dyn::Value
 */
static z::dyn::Value datum_to_dynamic(Datum value, Oid typid, bool isnull)
{
    if (isnull) {
        return z::dyn::Value();  // null value
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

        // TODO: Add support for more types:
        // - NUMERICOID (decimal numbers)
        // - DATEOID, TIMESTAMPOID (dates/timestamps)
        // - JSONOID, JSONBOID (nested JSON)
        // - Arrays
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
 * Generic helper function to convert PostgreSQL tuple to any binary format
 * Template parameter determines the serialization protocol
 */
template<typename Builder>
static bytea* tuple_to_binary(HeapTupleHeader rec)
{
    Oid tupType;
    int32 tupTypmod;
    TupleDesc tupdesc;
    HeapTupleData tuple;
    int ncolumns;

    // Extract type info from the record
    tupType = HeapTupleHeaderGetTypeId(rec);
    tupTypmod = HeapTupleHeaderGetTypMod(rec);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    // Build a temporary HeapTuple for attribute access
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    tuple.t_data = rec;

    ncolumns = tupdesc->natts;

    // Build serialized map
    Builder builder;

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

        // Convert to Dynamic and add to builder
        builder.add(std::string(attname), datum_to_dynamic(value, att->atttypid, isnull));
    }

    // Serialize
    z::ZBuffer buffer = builder.build();
    std::span<const uint8_t> data = buffer.buf();

    // Copy to PostgreSQL bytea
    size_t len = data.size();
    bytea* result = (bytea*) palloc(len + VARHDRSZ);
    SET_VARSIZE(result, len + VARHDRSZ);
    memcpy(VARDATA(result), data.data(), len);

    ReleaseTupleDesc(tupdesc);

    return result;
}

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
    result = tuple_to_binary<FlexBuffersBuilder>(rec);

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
    result = tuple_to_binary<MessagePackBuilder>(rec);

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
    result = tuple_to_binary<CBORBuilder>(rec);

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
    result = tuple_to_binary<ZERABuilder>(rec);

    PG_RETURN_BYTEA_P(result);
}
