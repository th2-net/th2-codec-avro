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

import com.exactpro.th2.common.grpc.*
import com.google.protobuf.ByteString
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.util.RandomData
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.File
import javax.xml.bind.DatatypeConverter


class TestAvroCodec {
    private val schema: Schema =
        this::class.java.classLoader.getResourceAsStream("schemas${File.separatorChar}test_schema.json")
            .use(Parser()::parse)
    private val codec = AvroCodec(schema, AvroCodecSettings())

    @Test
    fun `simple test decode`() {
        val rawBytes =
            DatatypeConverter.parseHexBinary(
                "F69BEDFE0CC48AEB888ACFE5C46700CA951A3FEA7A0EC04CEFE33F2A736373696A6A766476796C637870646D796169767004266A77786263796C71627877776A66736E6863652A746D74646D6C656B6C6E6976627070697866746F6F00021675706473626F706B7564640030766C6F6E656D67666B666A76676A6D71667075756D716575B4CBA9949DA99BB4CD01"
            )
        val body = ByteString.copyFrom(rawBytes)
        val rawMessage = RawMessage.newBuilder()
            .setMetadata(
                RawMessageMetadata.newBuilder()
                    .setId(MessageID.newBuilder().setSequence(1))
                    .setProtocol(AvroCodecFactory.PROTOCOL)
            )
            .setBody(body)
            .build()
        val group = MessageGroup.newBuilder().addMessages(AnyMessage.newBuilder().setRawMessage(rawMessage)).build()
        val decodeGroup = codec.decode(group)
        val decodeMessages = decodeGroup.messagesList
        check(decodeMessages.size == 1)
        val expectedCountFields = 9
        val actualCountFields = decodeMessages[0].message.fieldsMap.size
        check(actualCountFields == expectedCountFields) { "Expected count: $expectedCountFields not equal actual count: $actualCountFields" }
    }

    @Disabled
    @Test
    fun generateAvroRandomDataBySchema() {
        val writer = GenericDatumWriter<Any>()
        writer.setSchema(schema)
        val outputStream = ByteArrayOutputStream(8192)
        val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null)
        for (datum in RandomData(schema, 1)) {
            writer.write(datum, encoder)
        }
        encoder.flush()
        val data = outputStream.toByteArray()
        DatatypeConverter.printHexBinary(data)
        println(DatatypeConverter.printHexBinary(data))
    }
}