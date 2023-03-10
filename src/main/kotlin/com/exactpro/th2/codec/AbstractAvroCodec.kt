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
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.toJson
import com.google.protobuf.ByteString
import com.google.protobuf.UnsafeByteOperations
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import org.apache.avro.io.Decoder
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.io.IOException
import javax.xml.bind.DatatypeConverter

abstract class AbstractAvroCodec(
    settings: AvroCodecSettings,
) : IPipelineCodec {
    protected val enableIdPrefixEnumFields = settings.enableIdPrefixEnumFields

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

    abstract  fun decodeRawMessage(rawMessage: RawMessage, sessionAlias: String): Message

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

    abstract fun encodeMessage(parsedMessage: Message, sessionAlias: String): ByteString?

    protected fun getDecodedData(
        reader: MessageDatumReader,
        decoder: Decoder,
        rawMessage: RawMessage,
        bytes: ByteArray?,
        id: Any
    ): Message {
        try {
            return reader.read(Message.newBuilder(), decoder)
                .apply { if (rawMessage.hasParentEventId()) this.parentEventId = rawMessage.parentEventId }
                .setMetadata(
                    rawMessage.toMessageMetadataBuilder(listOf(AvroCodecFactory.PROTOCOL))
                        .setMessageType(reader.schema.name)
                )
                .build()

        } catch (e: IOException) {
            throw DecodeException(
                "Can't parse message data: ${DatatypeConverter.printHexBinary(bytes)} by schema id: $id",
                e
            )
        }
    }

    protected fun getEncodedData(
        writer: MessageDatumWriter,
        parsedMessage: Message,
        byteBuf: ByteBuf?
    ): ByteString? {
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder: Encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null)
        try {
            writer.write(parsedMessage, encoder)
        } catch (e: IOException) {
            throw IllegalStateException("Can't parse message data: ${parsedMessage.toJson()}", e)
        }
        encoder.flush()

        val header = UnsafeByteOperations.unsafeWrap(ByteBufUtil.getBytes(byteBuf))
        return header.concat(UnsafeByteOperations.unsafeWrap(byteArrayOutputStream.toByteArray()))
    }
}