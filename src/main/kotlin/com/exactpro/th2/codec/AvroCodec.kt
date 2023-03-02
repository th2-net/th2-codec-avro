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
import com.exactpro.th2.codec.resolver.AliasDatumResolver
import com.exactpro.th2.codec.resolver.SchemaIdDatumResolver
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.sessionAlias
import com.google.protobuf.ByteString
import com.google.protobuf.UnsafeByteOperations
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.*
import javax.xml.bind.DatatypeConverter
import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory

class AvroCodec(
    schemaIdToSchema: Map<Int, Schema>?,
    sessionAliasToSchema: Map<String, Schema>?,
    settings: AvroCodecSettings,
) : IPipelineCodec {
    private val standardMode = schemaIdToSchema != null
    private val enableIdPrefixEnumFields = settings.enableIdPrefixEnumFields
    private var schemaIdResolver: SchemaIdDatumResolver? = null
    private var aliasResolver: AliasDatumResolver? = null

    init {
        check(
            (schemaIdToSchema == null)
                .xor(sessionAliasToSchema == null)
        ) {
            "One of field schemaIdToSchema and schemaIdToSchema must be non-null"
        }

        if (standardMode) {
            schemaIdResolver = SchemaIdDatumResolver(schemaIdToSchema!!, enableIdPrefixEnumFields)
        } else {
            aliasResolver = AliasDatumResolver(sessionAliasToSchema!!, enableIdPrefixEnumFields)
        }
    }
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
                val sessionAlias = rawMessage.sessionAlias
                val decodeMessage = decodeRawMessage(rawMessage, sessionAlias)
                msgBuilder.addMessages(AnyMessage.newBuilder().setMessage(decodeMessage).build())
            }
        }
        return msgBuilder.build()
    }

    private fun decodeRawMessage(rawMessage: RawMessage, sessionAlias: String): Message {
        val bytes = rawMessage.body.toByteArray()
        val reader: MessageDatumReader
        val decoder: Decoder
        val id: Any
        if (standardMode) {
            val byteBuf = Unpooled.wrappedBuffer(bytes, 0, AVRO_HEADER_SIZE)
            val magicNumber: Byte = byteBuf.readByte()
            if (magicNumber.toInt() != MAGIC_BYTE_VALUE) {
                throw DecodeException(
                    "Message starts with not the magic value $MAGIC_BYTE_VALUE, data: ${
                        Arrays.toString(
                            bytes
                        )
                    }"
                )
            }
            val schemaId = byteBuf.readInt()
            reader = schemaIdResolver!!.getReader(schemaId)
            decoder = DecoderFactory.get().binaryDecoder(bytes, AVRO_HEADER_SIZE, bytes.size - AVRO_HEADER_SIZE, null)
            id = schemaId
        } else {
            check(sessionAlias.isNotEmpty()) {"Session alias cannot be empty. Raw message: $rawMessage"}
            reader = checkNotNull(aliasResolver!!.getReader(sessionAlias)) { "No reader found for session alias: $sessionAlias" }
            decoder = DecoderFactory.get().binaryDecoder(bytes, null)
            id = sessionAlias
        }
        try {
            return reader.read(Message.newBuilder(), decoder)
                .apply { if (rawMessage.hasParentEventId()) this.parentEventId = rawMessage.parentEventId }
                .setMetadata(
                    rawMessage.toMessageMetadataBuilder(listOf(AvroCodecFactory.PROTOCOL))
                        .setMessageType(checkNotNull(reader.schema.name) { "No message name found for id: $id" })
                )
                .build()

        } catch (e: IOException) {
            throw DecodeException("Can't parse message data: ${DatatypeConverter.printHexBinary(bytes)} by schema id: $id", e)
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
                val sessionAlias = parsedMessage.sessionAlias
                val messageBody = encodeMessage(parsedMessage, sessionAlias)
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

    private fun encodeMessage(parsedMessage: Message, sessionAlias: String): ByteString? {
        val byteBuf = Unpooled.buffer()
        val writer: MessageDatumWriter = if (standardMode) {
            val messageType =
                checkNotNull(parsedMessage.metadata.messageType) { "Message type is required. Message $parsedMessage does not have it" }
            val schemaId = schemaIdResolver!!.getSchemaId(messageType)
            byteBuf.writeByte(MAGIC_BYTE_VALUE)
            byteBuf.writeInt(schemaId)
            schemaIdResolver!!.getWriter(schemaId)
        } else {
            check(sessionAlias.isNotEmpty()) {"Session alias cannot be empty. Parsed message: $parsedMessage"}
            checkNotNull(aliasResolver!!.getWriter(sessionAlias)) { "No writer found for session alias: $sessionAlias" }
        }
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder: Encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null)
        try {
            writer.write(parsedMessage, encoder)
        } catch (e: IOException) {
            throw IllegalStateException("Can't parse message data: $parsedMessage}", e)
        }
        encoder.flush()

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
                .setId(metadata.id)
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