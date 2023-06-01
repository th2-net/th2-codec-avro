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

import com.exactpro.th2.codec.resolver.SchemaIdDatumResolver
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.google.protobuf.ByteString
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import com.exactpro.th2.common.grpc.Message as ProtoMessage
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage

class StandardAvroCodec(schemaIdToSchema: Map<Int, Schema>, settings: AvroCodecSettings) : AbstractAvroCodec(settings) {
    private val schemaIdResolver = SchemaIdDatumResolver(schemaIdToSchema, enableIdPrefixEnumFields)
    override fun decodeRawMessage(rawMessage: ProtoRawMessage, sessionAlias: String): ProtoMessage = decodeRawMessage(
        rawMessage,
        rawMessage.body.toByteArray(),
        schemaIdResolver::getReader,
        this::getDecodedData
    )

    override fun decodeRawMessage(rawMessage: RawMessage, sessionAlias: String): ParsedMessage = decodeRawMessage(
        rawMessage,
        rawMessage.body.toByteArray(),
        schemaIdResolver::getTransportReader,
        this::getDecodedData
    )

    private inline fun <S, R: GenericDatumReader<*>, T> decodeRawMessage(
        rawMessage: S,
        bytes: ByteArray,
        supplyReader: (Int) -> R,
        toParse: (R, Decoder, S, ByteArray, Int) -> T
    ): T {
        val byteBuf = Unpooled.wrappedBuffer(bytes, 0, AVRO_HEADER_SIZE)
        val magicNumber: Byte = byteBuf.readByte()
        if (magicNumber.toInt() != MAGIC_BYTE_VALUE) {
            throw DecodeException(
                "Message starts with not the magic value ${MAGIC_BYTE_VALUE}, data: ${bytes.contentToString()}"
            )
        }
        val schemaId = byteBuf.readInt()
        val reader = supplyReader(schemaId)
        val decoder = DecoderFactory.get().binaryDecoder(
            bytes,
            AVRO_HEADER_SIZE,
            bytes.size - AVRO_HEADER_SIZE,
            null
        )

        return toParse(reader, decoder, rawMessage, bytes, schemaId)
    }

    override fun encodeMessage(parsedMessage: ProtoMessage, sessionAlias: String): ByteString = encodeMessage(
        parsedMessage,
        checkNotBlank(parsedMessage.metadata.messageType) {
            "Message type is required. Message ${parsedMessage.toJson()} does not have it"
        },
        schemaIdResolver::getWriter,
        this::getEncodedData
    )

    override fun encodeMessage(parsedMessage: ParsedMessage, sessionAlias: String): ByteArray = encodeMessage(
        parsedMessage,
        checkNotBlank(parsedMessage.type) {
            "Message type is required. Message $parsedMessage does not have it"
        },
        schemaIdResolver::getTransportWriter,
        this::getEncodedData
    )

    private inline fun <S, W, T> encodeMessage(
        parsedMessage: S,
        messageType: String,
        supplyWriter: (Int) -> W,
        toRaw: (W, S, ByteBuf) -> T
    ): T {
        val byteBuf = Unpooled.buffer()
        val schemaId = schemaIdResolver.getSchemaId(messageType)
        byteBuf.writeByte(MAGIC_BYTE_VALUE)
        byteBuf.writeInt(schemaId)
        val writer = supplyWriter(schemaId)
        return toRaw(writer, parsedMessage, byteBuf)
    }

    companion object {
        const val AVRO_HEADER_SIZE = 5
        private const val MAGIC_BYTE_VALUE = 0

        private inline fun checkNotBlank(value: String, lazyMessage: () -> Any): String {
            require(value.isNotBlank(), lazyMessage)
            return value
        }
    }
}