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
import io.netty.buffer.Unpooled
import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.apache.commons.codec.EncoderException
import java.io.ByteArrayOutputStream
import java.io.IOException

class AvroCodec(
    private val schema: Schema,
    settings: AvroCodecSettings
) : IPipelineCodec {
    private val datumReader = MessageDatumReader(schema)
    private val datumWriter = MessageDatumWriter(schema)
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
                    .setParentEventId(rawMessage.parentEventId)
                    .setMetadata(
                        rawMessage.toMessageMetadataBuilder(listOf(AvroCodecFactory.PROTOCOL), 1)
                            .setMessageType(AVRO_MESSAGE)
                    )
                    .build()

                msgBuilder.addMessages(AnyMessage.newBuilder().setMessage(decodeMessage).build())
            }
        }
        return msgBuilder.build()
    }

    private fun decodeRawMessage(rawMessage: RawMessage): Message.Builder {
        val bytes = rawMessage.body.toByteArray()
        val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        try {
            return datumReader.read(Message.newBuilder(), decoder)
        } catch (e: IOException) {
            throw DecodeException("Can't parse message data: $bytes by schema: ${schema.fullName}", e)
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
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder: Encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null)

        try {
            datumWriter.write(parsedMessage, encoder)
        } catch (e: IOException) {
            throw EncoderException("Can't parse message data: $parsedMessage by schema: ${schema.fullName}", e)
        }
        encoder.flush()
        return UnsafeByteOperations.unsafeWrap(byteArrayOutputStream.toByteArray())
    }

    companion object {
        const val AVRO_MESSAGE = "AvroMessage"

        // FIXME remove after implementation toMessageMetadataBuilder(protocols: Collection<String>, subsequence: Int) in package com.exactpro.th2.codec.util
        fun RawMessage.toMessageMetadataBuilder(protocols: Collection<String>, subsequence: Int): MessageMetadata.Builder {
            val protocol = metadata.protocol.ifBlank {
                when(protocols.size) {
                    1 -> protocols.first()
                    else -> protocols.toString()
                }
            }

            return MessageMetadata.newBuilder()
                .setId(metadata.id.toBuilder().apply {
                    addSubsequence(subsequence)
                })
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