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

import com.exactpro.th2.codec.MessageDatumReader.Companion.getData
import org.apache.avro.*
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.Encoder
import org.apache.avro.path.*
import org.apache.avro.util.SchemaUtil
import java.io.IOException
import java.nio.ByteBuffer
import java.time.*

class TransportMessageDatumWriter(schema: Schema, private val enableIdPrefixEnumFields: Boolean = false) :
    GenericDatumWriter<MutableMap<String, Any>>(schema, getData()) {
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
        val map = (datum as Map<String, Any>)
        if (f.schema().type == Schema.Type.UNION) {
            val fieldName =
                map.keys.firstOrNull { s -> s.endsWith("$UNION_FIELD_NAME_TYPE_DELIMITER${f.name()}") }
            val unionValue = if (fieldName == null) null else map[fieldName]
            return Pair(resolveUnion(f.schema(), fieldName, unionValue), unionValue)
        }
        return map[f.name()]
    }

    @Throws(IOException::class)
    override fun writeRecord(schema: Schema, datum: Any?, out: Encoder) {
        for (field in schema.fields) {
            writeField(datum, field, out, null)
        }
    }

    @Throws(IOException::class)
    override fun writeWithoutConversion(schema: Schema, datum: Any, out: Encoder) {
        val schemaType = schema.type
        try {
            when (schemaType) {
                Schema.Type.RECORD -> writeRecord(schema, datum, out)
                Schema.Type.ENUM -> writeEnum(schema, datum, out)
                Schema.Type.ARRAY -> writeArray(schema, datum, out)
                Schema.Type.MAP -> writeMap(schema, datum, out)
                Schema.Type.UNION -> {
                    val (unionIndex, value) = datum as Pair<*, *>
                    out.writeIndex(unionIndex as Int)
                    if (value != null) {
                        write(schema.types[unionIndex], value, out)
                    }
                }

                Schema.Type.FIXED -> writeFixed(schema, datum, out)
                Schema.Type.STRING -> writeString(schema, datum as String, out)
                Schema.Type.BYTES -> writeBytes(datum, out)
                Schema.Type.INT -> {
                    when (datum) {
                        is Int -> out.writeInt(datum)
                        else -> throw AvroTypeException(
                            String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name)
                        )
                    }
                }

                Schema.Type.LONG -> {
                    when (datum) {
                        is Long -> out.writeLong(datum)
                        else -> throw AvroTypeException(
                            String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name)
                        )
                    }
                }

                Schema.Type.FLOAT -> out.writeFloat(datum as Float)
                Schema.Type.DOUBLE -> out.writeDouble(datum as Double)
                Schema.Type.BOOLEAN -> out.writeBoolean(datum as Boolean)
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
        out.writeEnum(schema.getEnumOrdinal(datum as String))
    }

    override fun getMapSize(map: Any): Int = (map as Map<*, *>).size
    override fun getMapEntries(map: Any): Iterable<Map.Entry<Any?, Any?>?> = (map as Map<*, *>).entries

    @Throws(IOException::class)
    override fun writeFixed(schema: Schema, datum: Any, out: Encoder) {
        out.writeFixed(datum as ByteArray, 0, schema.fixedSize)
    }

    @Throws(IOException::class)
    override fun writeBytes(datum: Any, out: Encoder) {
        when(datum){
            is ByteBuffer -> {
                val bytes = ByteArray(datum.remaining())
                datum.get(bytes)
                out.writeBytes(bytes)
            }
            is ByteArray -> {
                out.writeBytes(datum)
            }
            else -> throw AvroTypeException("Class ${datum::class.java} is not supported}")
        }
    }

    @Throws(IOException::class)
    override fun write(schema: Schema, datum: Any, out: Encoder) {
        val value = schema.logicalType?.let {
            convert(schema, it, data.getConversionByClass(datum.javaClass, it), datum)
        } ?: datum

        writeWithoutConversion(schema, value, out)
    }

    private fun resolveUnion(union: Schema, fieldName: String?, enumValue: Any?): Int {
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

    companion object {
        const val FORMAT_TYPE_ERROR = "Unsupported type %s for %s"
        const val UNION_FIELD_NAME_TYPE_DELIMITER = '-'
        const val UNION_ID_PREFIX = "Id"
    }
}