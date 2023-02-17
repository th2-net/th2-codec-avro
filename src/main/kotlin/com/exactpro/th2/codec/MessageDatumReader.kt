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

import com.exactpro.th2.codec.MessageDatumWriter.Companion.UNION_FIELD_NAME_TYPE_DELIMITER
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.value.toValue
import org.apache.avro.*
import org.apache.avro.data.TimeConversions.*
import org.apache.avro.generic.*
import org.apache.avro.io.Decoder
import org.apache.avro.io.ResolvingDecoder
import java.io.IOException
import java.nio.ByteBuffer
import java.util.*
import javax.xml.bind.DatatypeConverter

class MessageDatumReader(schema: Schema) :
    GenericDatumReader<Message.Builder>(schema, schema, getData()) {
    @Throws(IOException::class)
    override fun readWithoutConversion(old: Any?, expected: Schema, decoder: ResolvingDecoder): Any? {
        return if (expected.type == Schema.Type.UNION) {
            val readIndex = decoder.readIndex()
            val schema = expected.types[readIndex]
            UnionData(read(old, schema, decoder), schema.name)
        } else {
            super.readWithoutConversion(old, expected, decoder)
        }
    }

    @Throws(IOException::class)
    override fun readRecord(old: Any?, expected: Schema, decoder: ResolvingDecoder): Message.Builder {
        val r = createRecord()
        for (f in decoder.readFieldOrder()) {
            readField(r, f, old, decoder, null)
        }
        return r
    }

    @Throws(IOException::class)
    override fun readField(r: Any, f: Schema.Field, oldDatum: Any?, decoder: ResolvingDecoder, state: Any?) {
        var readValue = read(oldDatum, f.schema(), decoder)
        var fieldName = f.name()

        if (readValue is UnionData) {
            val description = readValue.description
            readValue = readValue.value
            if (readValue != null) {
                fieldName = resolveUnionFieldName(fieldName, description)
            }
        }
        if (readValue != null) {
            (r as Message.Builder).addField(fieldName, readValue.convertToValue())
        }
    }

    private fun resolveUnionFieldName(fieldName: String, description: String): String {
        return "$description$UNION_FIELD_NAME_TYPE_DELIMITER$fieldName"
    }

    private fun createRecord(): Message.Builder {
        return Message.newBuilder()
    }

    @Throws(IOException::class)
    override fun readString(old: Any?, expected: Schema, decoder: Decoder): String {
        return super.readString(old, expected, decoder).toString()
    }

    @Throws(IOException::class)
    override fun readEnum(expected: Schema, decoder: Decoder): String {
        return expected.enumSymbols[decoder.readEnum()]
    }

    override fun addToMap(map: Any, key: Any?, value: Any?) {
        if (value != null) {
            (map as Message.Builder).addField(key.toString(), value.convertToValue())
        }
    }

    override fun newMap(old: Any?, size: Int): Message.Builder {
        return Message.newBuilder()
    }

    private fun ByteBuffer.asHexString(): String {
        val bytes = ByteArray(this.remaining())
        this.get(bytes)
        return byteArrayToHEXString(bytes)
    }
    private fun GenericFixed.asHexString(): String = byteArrayToHEXString(this.bytes())
    private fun Any.convertToValue(): Value = when (this) {
        is ByteBuffer ->
            this.asHexString().toValue()
        is GenericFixed ->
            this.asHexString().toValue()
        else -> toValue()
    }
    data class UnionData(
        val value: Any?,
        val description: String
    )
    companion object {
         fun byteArrayToHEXString(bytes: ByteArray): String = DatatypeConverter.printHexBinary(bytes)
        fun getData(): GenericData? {
            return GenericData.get().apply {
                addLogicalTypeConversion(Conversions.DecimalConversion())
                addLogicalTypeConversion(DateConversion())
                addLogicalTypeConversion(TimeMillisConversion())
                addLogicalTypeConversion(TimeMicrosConversion())
                addLogicalTypeConversion(TimestampMillisConversion())
                addLogicalTypeConversion(TimestampMicrosConversion())
                addLogicalTypeConversion(LocalTimestampMillisConversion())
                addLogicalTypeConversion(LocalTimestampMicrosConversion())
            }
        }
    }
}
