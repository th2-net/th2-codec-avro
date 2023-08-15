/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.getField
import com.exactpro.th2.common.value.getList
import com.exactpro.th2.common.value.getMessage
import org.apache.avro.UnresolvedUnionException
import org.apache.avro.AvroTypeException
import org.apache.avro.Schema
import org.apache.avro.Conversion
import org.apache.avro.io.Encoder
import org.apache.avro.path.TracingAvroTypeException
import org.apache.avro.path.TracingClassCastException
import org.apache.avro.path.TracingNullPointException
import org.apache.avro.path.LocationStep
import org.apache.avro.util.SchemaUtil
import java.io.IOException
import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.LocalTime
import java.time.LocalDateTime
import java.time.Instant
import javax.xml.bind.DatatypeConverter
import com.exactpro.th2.codec.AbstractMessageWriter.Companion.Type

class MessageDatumWriter(schema: Schema, private val enableIdPrefixEnumFields: Boolean = false) :
    AbstractMessageWriter<Message>(schema) {
    @Throws(IOException::class)
    override fun writeField(datum: Any?, f: Schema.Field, out: Encoder, state: Any?) {
        val value = resolveUnionValue(f, datum)
        if (value != null) {
            try {
                write(f.schema(), value, out)
            } catch (e: Exception) {
                when (e) {
                    is UnresolvedUnionException -> throw UnresolvedUnionException(f.schema(), f, value).apply { addSuppressed(e) }
                    is TracingNullPointException -> throw e.apply {tracePath(LocationStep(".", f.name()))}
                    is TracingClassCastException -> throw e.apply {tracePath(LocationStep(".", f.name()))}
                    is TracingAvroTypeException -> throw e.apply {tracePath(LocationStep(".", f.name()))}
                    is NullPointerException -> throw npe(e, " in field ${f.name()}")
                    is ClassCastException -> throw addClassCastMsg(e, " in field ${f.name()}")
                    is AvroTypeException -> throw addAvroTypeMsg(e, " in field ${f.name()}")
                    else -> throw e
                }
            }
        }
    }

    private fun resolveUnionValue(f: Schema.Field, datum: Any?): Any? {
        if (f.schema().type == Schema.Type.UNION) {
            val fieldName =
                (datum as Message).fieldsMap.keys.firstOrNull { s -> s.endsWith("$UNION_FIELD_NAME_TYPE_DELIMITER${f.name()}") }
            val unionValue = if (fieldName == null) null else datum.getField(fieldName)
            return Pair(resolveUnion(f.schema(), fieldName, unionValue), unionValue)

        }
        return (datum as Message).getField(f.name())
    }

    @Throws(IOException::class)
    override fun writeRecord(schema: Schema, datum: Any?, out: Encoder) {
        for (field in schema.fields) {
            writeField(if (datum is Value && datum.hasMessageValue()) datum.getMessage() else datum, field, out, null)
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
                Schema.Type.UNION -> {
                    val (unionIndex, value) = datum as Pair<*, *>
                    out.writeIndex(unionIndex as Int)
                    if (value != null) {
                        write(schema.types[unionIndex], value, out)
                    }
                }
                Schema.Type.FIXED -> writeFixed(schema, datum, out)
                Schema.Type.STRING -> if (datum is Value) writeString(schema, datum.simpleValue.toString(), out)
                Schema.Type.BYTES -> writeBytes(datum, out)
                Schema.Type.INT -> {
                    when (datum) {
                        is Value -> out.writeInt(datum.simpleValue.toInt())
                        is Int -> out.writeInt(datum)
                        else -> throw AvroTypeException(String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name))
                    }
                }

                Schema.Type.LONG -> {
                    when (datum) {
                        is Value -> out.writeLong(datum.simpleValue.toLong())
                        is Long -> out.writeLong(datum)
                        else -> throw AvroTypeException(String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name))
                    }

                }

                Schema.Type.FLOAT -> out.writeFloat((datum as Value).simpleValue.toFloat())
                Schema.Type.DOUBLE -> out.writeDouble((datum as Value).simpleValue.toDouble())
                Schema.Type.BOOLEAN -> out.writeBoolean((datum as Value).simpleValue.toBoolean())
                Schema.Type.NULL -> out.writeNull()
                else -> throw AvroTypeException(
                    "Value ${SchemaUtil.describe(datum)} is not a ${SchemaUtil.describe(schema)}"
                )
            }
        } catch (e: Exception) {
            when (e) {
                is NullPointerException -> throw TracingNullPointException(e, schema, false)
                is ClassCastException -> throw TracingClassCastException(e, datum, schema, false)
                is AvroTypeException -> throw TracingAvroTypeException(e)
                else -> throw e
            }
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
        out.writeFixed(DatatypeConverter.parseHexBinary((datum as Value).simpleValue.toString()), 0, schema.fixedSize)
    }

    @Throws(IOException::class)
    override fun writeBytes(datum: Any, out: Encoder) {
        when(datum){
            is Value -> out.writeBytes(DatatypeConverter.parseHexBinary(datum.simpleValue.toString()))
            is ByteBuffer -> {
                val bytes = ByteArray(datum.remaining())
                datum.get(bytes)
                out.writeBytes(bytes)
            }
        }
    }

    @Throws(IOException::class)
    override fun write(schema: Schema, datum: Any, out: Encoder) {
        val logicalType = schema.logicalType
        if (logicalType != null) {
            val simpleValue = (datum as Value).simpleValue
            val convertedValue = when (logicalType.name) {
                Type.DECIMAL.type ->  simpleValue.toBigDecimal()
                Type.DATE.type -> LocalDate.parse(simpleValue.toString())
                Type.TIME_MILLIS.type -> LocalTime.parse(simpleValue.toString(), localTimeWithMillisConverter)
                Type.TIME_MICROS.type -> LocalTime.parse(simpleValue.toString(), localTimeWithMicrosConverter)
                Type.TIMESTAMP_MILLIS.type,
                Type.TIMESTAMP_MICROS.type -> Instant.parse(simpleValue.toString())
                Type.LOCAL_TIMESTAMP_MILLIS.type -> LocalDateTime.parse(simpleValue.toString(), localDateTimeWithMillisConverter)
                Type.LOCAL_TIMESTAMP_MICROS.type -> LocalDateTime.parse(simpleValue.toString(), localDateTimeWithMicrosConverter)
                else ->  throw AvroTypeException("Logical type ${logicalType.name} is not supported}")
            }
            val conversion: Conversion<*> = data.getConversionByClass(convertedValue.javaClass, logicalType)
            writeWithoutConversion(schema, convert(schema, logicalType, conversion, convertedValue), out)
        } else {
            writeWithoutConversion(schema, datum, out)
        }
    }
    private fun resolveUnion(union: Schema, fieldName: String?, enumValue: Value?): Int {
        if (enableIdPrefixEnumFields.and(enumValue != null)) {
            try {
                return checkNotNull(
                    fieldName?.substringBefore(UNION_FIELD_NAME_TYPE_DELIMITER)?.substringAfter(UNION_ID_PREFIX)
                        ?.toInt()
                ) { "Schema id not found in field name: $fieldName" }
            } catch (e: NumberFormatException) {
                throw AvroTypeException(
                    "Union prefix: $UNION_ID_PREFIX'{schema id}'$UNION_FIELD_NAME_TYPE_DELIMITER not found in field name: $fieldName",
                    e
                )
            }
        }
        val schemaName = checkNotNull(
            if (enumValue == null) Schema.Type.NULL.getName() else fieldName?.substringBefore(
                UNION_FIELD_NAME_TYPE_DELIMITER
            )
        ) { "Union prefix: {avro type}$UNION_FIELD_NAME_TYPE_DELIMITER not found in field name: $fieldName" }
        return checkNotNull(union.getIndexNamed(schemaName)) { "Schema with name: $schemaName not found in union parent schema: ${union.name}" }
    }
}