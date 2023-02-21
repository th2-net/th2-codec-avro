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

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.google.protobuf.ByteString
import com.google.protobuf.UnsafeByteOperations
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.commons.codec.EncoderException
import org.apache.commons.codec.binary.Hex
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.*

class AvroCodec(
    private val schemaIdToSchema: Map<Int, Schema>,
    settings: AvroCodecSettings
) : IPipelineCodec {
    private val enableIdPrefixEnumFields = settings.enableIdPrefixEnumFields
    private val datumReaders: Map<Int, MessageDatumReader> = schemaIdToSchema.mapValues { MessageDatumReader(it.value, enableIdPrefixEnumFields) }
    private val datumWriters: Map<Int, MessageDatumWriter> = schemaIdToSchema.mapValues { MessageDatumWriter(it.value, enableIdPrefixEnumFields) }
    private val schemaIdToMessageName: Map<Int, String> = schemaIdToSchema.mapValues { it.value.name }
    private val messageNameToSchemaId: Map<String, Int> =
        schemaIdToMessageName.entries.associate { (key, value) -> value to key }
    override fun decode(messageGroup: MessageGroup): MessageGroup {
        val messages = messageGroup.messagesList


        if (messages.isEmpty().or(messages.stream().allMatch(AnyMessage::hasMessage))) {
            return messageGroup
        }

        val msgBuilder = MessageGroup.newBuilder()
        messages.forEach { message ->
            if (!message.hasRawMessage().or(message.message.metadata.run {
                    protocol.isNotEmpty().and(protocol != AvroCodecFactory.PROTOCOL)
                })) {
                msgBuilder.addMessages(message)
            } else {
                val rawMessage = message.rawMessage
                val decodeMessage = decodeRawMessage(rawMessage)

                msgBuilder.addMessages(AnyMessage.newBuilder().setMessage(decodeMessage).build())
            }
        }
        return msgBuilder.build()
    }

    private fun decodeRawMessage(rawMessage: RawMessage): Message {
        val bytes = rawMessage.body.toByteArray()
        val byteBuf = Unpooled.wrappedBuffer(bytes, 0, AVRO_HEADER_SIZE)
        val magicNumber: Byte = byteBuf.readByte()
        if (magicNumber.toInt() != MAGIC_BYTE_VALUE) {
            throw DecodeException("Message starts with not the magic value $MAGIC_BYTE_VALUE, data: ${Arrays.toString(bytes)}")
        }
        val schemaId: Int = byteBuf.readInt()
        val reader = checkNotNull(datumReaders[schemaId]) { "No reader found for schema id: $schemaId" }
        val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, AVRO_HEADER_SIZE, bytes.size - AVRO_HEADER_SIZE, null)
        try {
            return reader.read(Message.newBuilder(), decoder)
                .apply { if (rawMessage.hasParentEventId()) this.parentEventId = rawMessage.parentEventId }
                .setMetadata(
                    rawMessage.toMessageMetadataBuilder(listOf(AvroCodecFactory.PROTOCOL))
                        .setMessageType(checkNotNull(schemaIdToMessageName[schemaId]) { "No message name found for schema id: $schemaId" })
                )
                .build()

        } catch (e: IOException) {
            throw DecodeException("Can't parse message data: ${Hex.encodeHexString(bytes)} by schema id: $schemaId", e)
        }
    }

    override fun encode(messageGroup: MessageGroup): MessageGroup {
        val messages = messageGroup.messagesList

        if (messages.isEmpty().or(messages.stream().allMatch(AnyMessage::hasRawMessage))) {
            return messageGroup
        }

        val msgBuilder = MessageGroup.newBuilder()
        Unpooled.buffer()
        messages.forEach { message ->
            if (!message.hasMessage().or(message.message.metadata.run {
                    protocol.isNotEmpty().and(protocol != AvroCodecFactory.PROTOCOL)
                })) {
                msgBuilder.addMessages(message)
            } else {
                val parsedMessage = message.message
                val messageBody = encodeMessage(parsedMessage)
                val rawMessage = RawMessage.newBuilder()
                    .setMetadata(
                        parsedMessage.toRawMetadataBuilder(listOf(AvroCodecFactory.PROTOCOL))
                    )
                    .setBody(messageBody)
                    .build()

                msgBuilder.addMessages(AnyMessage.newBuilder().setRawMessage(rawMessage).build())

            }
        }
        return msgBuilder.build()
    }

    private fun encodeMessage(parsedMessage: Message): ByteString? {
        val messageType = checkNotNull(parsedMessage.metadata.messageType) { "Message type is required. Message $parsedMessage does not have it" }
        val schemaId = checkNotNull(messageNameToSchemaId[messageType]) { "No schema id found for message type: $messageType" }
        val writer = checkNotNull(datumWriters[schemaId]) { "No writer found for schema id: $schemaId" }
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder: Encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null)
        try {
            writer.write(parsedMessage, encoder)
        } catch (e: IOException) {
            throw EncoderException("Can't parse message data: $parsedMessage by schema: ${schemaIdToSchema[schemaId]}", e)
        }
        encoder.flush()
        val byteBuf = Unpooled.buffer()
        byteBuf.writeByte(MAGIC_BYTE_VALUE)
        byteBuf.writeInt(schemaId)
        val header = UnsafeByteOperations.unsafeWrap(ByteBufUtil.getBytes(byteBuf))
        return header.concat(UnsafeByteOperations.unsafeWrap(byteArrayOutputStream.toByteArray()))
    }

    companion object {
        const val AVRO_HEADER_SIZE = 5
        private const val MAGIC_BYTE_VALUE = 0

        // FIXME remove after implementation toMessageMetadataBuilder(protocols: Collection<String>, subsequence: Int) in package com.exactpro.th2.codec.util
        fun RawMessage.toMessageMetadataBuilder(protocols: Collection<String>): MessageMetadata.Builder {
            val protocol = metadata.protocol.ifBlank {
                when(protocols.size) {
                    1 -> protocols.first()
                    else -> protocols.toString()
                }
            }

            return MessageMetadata.newBuilder()
                .setTimestamp(metadata.timestamp)
                .setProtocol(protocol)
                .putAllProperties(metadata.propertiesMap)
        }
        //FIXME: move to core part
        fun Message.toRawMetadataBuilder(protocols: Collection<String>): RawMessageMetadata.Builder {
            val protocol = metadata.protocol.ifBlank {
                when(protocols.size) {
                    1 -> protocols.first()
                    else -> protocols.toString()
                }
            }

            return RawMessageMetadata.newBuilder()
                .setId(metadata.id)
                .setTimestamp(metadata.timestamp)
                .setProtocol(protocol)
                .putAllProperties(metadata.propertiesMap)
        }

    }
}