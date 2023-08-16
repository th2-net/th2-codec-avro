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

import org.apache.avro.Schema
import org.apache.avro.AvroTypeException
import org.apache.avro.UnresolvedUnionException
import org.apache.avro.io.Encoder
import org.apache.avro.path.TracingAvroTypeException
import org.apache.avro.path.TracingNullPointException
import org.apache.avro.path.TracingClassCastException
import org.apache.avro.path.LocationStep
import org.apache.avro.util.SchemaUtil
import java.io.IOException
import java.nio.ByteBuffer
import javax.xml.bind.DatatypeConverter

class TransportMessageDatumWriter(schema: Schema, enableIdPrefixEnumFields: Boolean = false) :
    AbstractMessageWriter<Map<String, Any?>>(schema, enableIdPrefixEnumFields) {
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
        @Suppress("UNCHECKED_CAST")
        val map = (datum as Map<String, Any?>)
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

                Schema.Type.FIXED -> {
                    val bytes = when (datum) {
                        is ByteArray -> datum
                        is String -> DatatypeConverter.parseHexBinary(datum)
                        else -> throw AvroTypeException(String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name))
                    }
                    writeFixed(schema, bytes, out)
                }
                Schema.Type.STRING -> writeString(schema, datum as String, out)
                Schema.Type.BYTES -> writeBytes(datum, out)
                Schema.Type.INT -> {
                    val int = when (datum) {
                        is Int -> datum
                        is String -> datum.toInt()
                        else -> throw AvroTypeException(String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name))
                    }
                    out.writeInt(int)
                }

                Schema.Type.LONG -> {
                    val long = when (datum) {
                        is Long -> datum
                        is String -> datum.toLong()
                        else -> throw AvroTypeException(String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name))
                    }
                    out.writeLong(long)
                }

                Schema.Type.FLOAT -> {
                    val float = when (datum) {
                        is Float -> datum
                        is String -> datum.toFloat()
                        else -> throw AvroTypeException(String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name))
                    }
                    out.writeFloat(float)
                }

                Schema.Type.DOUBLE -> {
                    val double = when (datum) {
                        is Double -> datum
                        is String -> datum.toDouble()
                        else -> throw AvroTypeException(String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name))
                    }
                    out.writeDouble(double)
                }

                Schema.Type.BOOLEAN -> {
                    val boolean = when (datum) {
                        is Boolean -> datum
                        is String -> datum.toBoolean()
                        else -> throw AvroTypeException(String.format(FORMAT_TYPE_ERROR, datum.javaClass, schemaType.name))
                    }
                    out.writeBoolean(boolean)
                }

                Schema.Type.NULL -> out.writeNull()
                else -> throw AvroTypeException("Value ${SchemaUtil.describe(datum)} is not a ${SchemaUtil.describe(schema)}")
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
            is ByteArray -> out.writeBytes(datum)
            is String -> out.writeBytes(DatatypeConverter.parseHexBinary(datum))
            else -> throw AvroTypeException("Class ${datum::class.java} is not supported}")
        }
    }

    @Throws(IOException::class)
    override fun write(schema: Schema, datum: Any, out: Encoder) {
        val value = schema.logicalType?.let {
            val convertedValue = if (datum is String) {
                convertFieldValue(datum, it.name)
            } else {
                datum
            }
            convert(schema, it, data.getConversionByClass(convertedValue.javaClass, it), convertedValue)
        } ?: datum

        writeWithoutConversion(schema, value, out)
    }
}