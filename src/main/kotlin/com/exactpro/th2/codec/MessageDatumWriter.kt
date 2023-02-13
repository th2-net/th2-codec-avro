package com.exactpro.th2.codec

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.value.getList
import com.exactpro.th2.common.value.getMessage
import org.apache.avro.AvroTypeException
import org.apache.avro.Schema
import org.apache.avro.UnresolvedUnionException
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.Encoder
import org.apache.avro.path.*
import org.apache.avro.util.SchemaUtil
import java.io.IOException
import javax.xml.bind.DatatypeConverter

class MessageDatumWriter(schema: Schema?) :
    GenericDatumWriter<Message>(schema, null) {
    @Throws(IOException::class)
    override fun writeField(datum: Any?, f: Schema.Field, out: Encoder?, state: Any?) {
        val value = (datum as Message).getField(f.name())
        if (value != null) {
            try {
                write(f.schema(), value, out)
            } catch (uue: UnresolvedUnionException) { // recreate it with the right field info
                val unresolvedUnionException = UnresolvedUnionException(f.schema(), f, value)
                unresolvedUnionException.addSuppressed(uue)
                throw unresolvedUnionException
            } catch (e: TracingNullPointException) {
                e.tracePath(LocationStep(".", f.name()))
                throw e
            } catch (e: TracingClassCastException) {
                e.tracePath(LocationStep(".", f.name()))
                throw e
            } catch (e: TracingAvroTypeException) {
                e.tracePath(LocationStep(".", f.name()))
                throw e
            } catch (e: NullPointerException) {
                throw npe(e, " in field " + f.name())
            } catch (cce: ClassCastException) {
                throw addClassCastMsg(cce, " in field " + f.name())
            } catch (ate: AvroTypeException) {
                throw addAvroTypeMsg(ate, " in field " + f.name())
            }
        }
    }

    @Throws(IOException::class)
    override fun writeRecord(schema: Schema, datum: Any?, out: Encoder?) {
        for (f in schema.fields) {
            writeField(if (datum is Value) datum.getMessage() else datum, f!!, out, null)
        }
    }

    @Throws(IOException::class)
    override fun writeWithoutConversion(schema: Schema, datum: Any, out: Encoder) {
        val schemaType = schema.type
        try {
            when (schemaType) {
                Schema.Type.RECORD -> writeRecord(schema, datum, out)
                Schema.Type.ENUM -> writeEnum(schema, datum, out)
                Schema.Type.ARRAY -> writeArray(schema, (datum as Value).getList(), out)
                Schema.Type.MAP -> writeMap(schema, (datum as Value).getMessage(), out)
                Schema.Type.UNION -> throw AvroTypeException(
                    "$schemaType type is not supported in encoding, not enough data type information"
                )

                Schema.Type.FIXED -> writeFixed(schema, datum, out)
                Schema.Type.STRING -> writeString(schema, (datum as Value).simpleValue.toString(), out)
                Schema.Type.BYTES -> writeBytes(datum, out)
                Schema.Type.INT -> out.writeInt((datum as Value).simpleValue.toInt())
                Schema.Type.LONG -> out.writeLong((datum as Value).simpleValue.toLong())
                Schema.Type.FLOAT -> out.writeFloat((datum as Value).simpleValue.toFloat())
                Schema.Type.DOUBLE -> out.writeDouble((datum as Value).simpleValue.toDouble())
                Schema.Type.BOOLEAN -> out.writeBoolean((datum as Value).simpleValue.toBoolean())
                Schema.Type.NULL -> out.writeNull()
                else -> throw AvroTypeException(
                    "Value ${SchemaUtil.describe(datum)} is not a ${SchemaUtil.describe(schema)}"
                )
            }
        } catch (e: NullPointerException) {
            throw TracingNullPointException(e, schema, false)
        } catch (e: ClassCastException) {
            throw TracingClassCastException(e, datum, schema, false)
        } catch (e: AvroTypeException) {
            throw TracingAvroTypeException(e)
        }
    }

    @Throws(IOException::class)
    override fun writeEnum(schema: Schema, datum: Any, out: Encoder) {
        out.writeEnum(schema.getEnumOrdinal((datum as Value).simpleValue.toString()))
    }

    override fun getMapSize(map: Any): Int {
        return (map as Message).fieldsCount
    }

    override fun getMapEntries(map: Any): Iterable<Map.Entry<Any?, Any?>?> {
        return (map as Message).fieldsMap.entries
    }

    @Throws(IOException::class)
    override fun writeFixed(schema: Schema, datum: Any, out: Encoder) {
        out.writeFixed(hexStringToByteArray((datum as Value).simpleValue.toString()), 0, schema.fixedSize)
    }

    @Throws(IOException::class)
    override fun writeBytes(datum: Any?, out: Encoder) {
        out.writeBytes(hexStringToByteArray((datum as Value).simpleValue.toString()))
    }

    private fun hexStringToByteArray(hexString: String) = DatatypeConverter.parseHexBinary(hexString)
}