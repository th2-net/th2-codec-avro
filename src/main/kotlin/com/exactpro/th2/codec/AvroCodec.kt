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
import com.exactpro.th2.codec.util.toMessageMetadataBuilder
import com.exactpro.th2.codec.util.toRawMetadataBuilder
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.RawMessage
import com.google.protobuf.ByteString
import io.netty.buffer.Unpooled
import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import java.io.IOException
import java.nio.charset.StandardCharsets

class AvroCodec(
    private val schema: Schema,
    settings: AvroCodecSettings
) : IPipelineCodec {
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
                val decodeMessage = getMessageBuilder(rawMessage)
                    .setParentEventId(rawMessage.parentEventId)
                    .setMetadata(
                        rawMessage.toMessageMetadataBuilder(listOf(AvroCodecFactory.PROTOCOL))
                            .setMessageType(AVRO_MESSAGE)
                    )
                    .build()

                msgBuilder.addMessages(AnyMessage.newBuilder().setMessage(decodeMessage).build())
            }
        }
        return msgBuilder.build()
    }

    private fun getMessageBuilder(rawMessage: RawMessage): Message.Builder {
        val bytes = rawMessage.body.toByteArray()
        val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        val datumReader = MessageDatumReader(schema)
        try {
            return datumReader.read(Message.newBuilder(), decoder)
        } catch (e: IOException) {
            throw DecodeException("Can't parse message data: $bytes by schema: ${schema.fullName}")
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
                val messageBody = getMessageBody(parsedMessage)
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

    private fun getMessageBody(parsedMessage: Message): ByteString? {
        val fieldsMap = parsedMessage.fieldsMap
        val messageBody = ByteString.copyFrom(fieldsMap[AVRO_MESSAGE].toString(), StandardCharsets.UTF_8)
        return messageBody
    }

    companion object {
        const val AVRO_MESSAGE = "AvroMessage"
    }
}