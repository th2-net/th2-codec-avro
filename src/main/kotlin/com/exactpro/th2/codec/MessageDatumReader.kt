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
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.value.toValue
import org.apache.avro.Conversion
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.io.ResolvingDecoder
import java.io.IOException
import java.nio.ByteBuffer
import javax.xml.bind.DatatypeConverter

class MessageDatumReader(schema: Schema?) :
    GenericDatumReader<Message.Builder>(schema) {

    @Throws(IOException::class)
    override fun readWithConversion(
        old: Any?, expected: Schema?, logicalType: LogicalType?, conversion: Conversion<*>?,
        decoder: ResolvingDecoder?
    ): Any? {
        throw IllegalStateException("Logical types not supported")
    }

    @Throws(IOException::class)
    override fun read(old: Any?, expected: Schema, decoder: ResolvingDecoder?): Any? {
        val datum = readWithoutConversion(old, expected, decoder)
        val logicalType = expected.logicalType
        check(logicalType == null) { "Logical types not supported" }
        return datum
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
        val readValue = read(oldDatum, f.schema(), decoder)
        if (readValue != null) {
            (r as Message.Builder).addField(f.name(), readValue.toValue())
        }
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
            (map as Message.Builder).addField(key.toString(), value.toValue())
        }
    }

    override fun newMap(old: Any?, size: Int): Message.Builder {
        return Message.newBuilder()
    }

    @Throws(IOException::class)
    override fun readFixed(old: Any?, expected: Schema, decoder: Decoder): String {
        val bytes = ByteArray(expected.fixedSize)
        decoder.readFixed(bytes, 0, expected.fixedSize)
        return byteArrayToHEXString(bytes)
    }

    private fun byteArrayToHEXString(bytes: ByteArray) = DatatypeConverter.printHexBinary(bytes)

    @Throws(IOException::class)
    override fun readBytes(old: Any?, decoder: Decoder): String {
        val buffer = super.readBytes(old, decoder) as ByteBuffer
        val bytes = ByteArray(buffer.remaining())
        buffer.get(bytes)
        return byteArrayToHEXString(bytes)
    }
}