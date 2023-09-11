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
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.util.toJson
import com.exactpro.th2.codec.util.toMessageMetadataBuilder
import com.exactpro.th2.codec.util.toRawMetadataBuilder
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
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
import com.exactpro.th2.common.grpc.AnyMessage as ProtoAnyMessage
import com.exactpro.th2.common.grpc.Message as ProtoMessage
import com.exactpro.th2.common.grpc.MessageGroup as ProtoMessageGroup
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage

abstract class AbstractAvroCodec(
    settings: AvroCodecSettings,
) : IPipelineCodec {
    protected val enableIdPrefixEnumFields: Boolean = settings.enableIdPrefixEnumFields
    protected val enablePrefixEnumFieldsDecode: Boolean? = if (settings.enablePrefixEnumFieldsDecode) {
        enableIdPrefixEnumFields
    } else {
        null
    }

    override fun decode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup {
        val msgBuilder = ProtoMessageGroup.newBuilder()
        messageGroup.messagesList.forEach { message ->
            if (!message.hasRawMessage().or(message.message.metadata.run {
                    protocol.isNotEmpty().and(protocol != AvroCodecFactory.PROTOCOL)
                })) {
                msgBuilder.addMessages(message)
            } else {
                val rawMessage = message.rawMessage
                val sessionAlias = rawMessage.sessionAlias
                val decodeMessage = decodeRawMessage(rawMessage, sessionAlias)
                msgBuilder.addMessages(ProtoAnyMessage.newBuilder().setMessage(decodeMessage).build())
            }
        }
        return msgBuilder.build()
    }

    override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup {
        return MessageGroup.builder().apply {
            messageGroup.messages.forEach { message ->
                addMessage(
                    if (message !is RawMessage || (message.protocol.isNotEmpty() && (message.protocol != AvroCodecFactory.PROTOCOL))) {
                        message
                    } else {
                        decodeRawMessage(message, message.id.sessionAlias)
                    }
                )
            }
        }.build()
    }

    abstract fun decodeRawMessage(rawMessage: ProtoRawMessage, sessionAlias: String): ProtoMessage
    abstract fun decodeRawMessage(rawMessage: RawMessage, sessionAlias: String): ParsedMessage

    override fun encode(messageGroup: ProtoMessageGroup, context: IReportingContext): ProtoMessageGroup {
        val msgBuilder = ProtoMessageGroup.newBuilder()
        Unpooled.buffer()
        messageGroup.messagesList.forEach { message ->
            if (!message.hasMessage().or(message.message.metadata.run {
                    protocol.isNotEmpty().and(protocol != AvroCodecFactory.PROTOCOL)
                })) {
                msgBuilder.addMessages(message)
            } else {
                val parsedMessage = message.message
                val sessionAlias = parsedMessage.sessionAlias
                val messageBody = encodeMessage(parsedMessage, sessionAlias)
                val rawMessage = ProtoRawMessage.newBuilder()
                    .apply { if (parsedMessage.hasParentEventId()) this.parentEventId = parsedMessage.parentEventId }
                    .setMetadata(
                        parsedMessage.toRawMetadataBuilder(listOf(AvroCodecFactory.PROTOCOL))
                    )
                    .setBody(messageBody)
                    .build()

                msgBuilder.addMessages(ProtoAnyMessage.newBuilder().setRawMessage(rawMessage).build())

            }
        }
        return msgBuilder.build()
    }

    override fun encode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup {
        return MessageGroup.builder().apply {
            messageGroup.messages.forEach { message ->
                addMessage(
                    if (message !is ParsedMessage || message.protocol.isNotEmpty() && message.protocol != AvroCodecFactory.PROTOCOL) {
                        message
                    } else {
                        val sessionAlias = message.id.sessionAlias
                        val messageBody = encodeMessage(message, sessionAlias)
                        RawMessage.builder().apply {
                            setId(message.id)
                            message.eventId?.let(this::setEventId)
                            setMetadata(message.metadata)
                            setProtocol(AvroCodecFactory.PROTOCOL)
                            setBody(Unpooled.wrappedBuffer(messageBody))
                        }.build()
                    }
                )
            }
        }.build()
    }

    abstract fun encodeMessage(parsedMessage: ProtoMessage, sessionAlias: String): ByteString
    abstract fun encodeMessage(parsedMessage: ParsedMessage, sessionAlias: String): ByteArray

    protected fun getDecodedData(
        reader: MessageDatumReader,
        decoder: Decoder,
        rawMessage: ProtoRawMessage,
        bytes: ByteArray,
        id: Any
    ): ProtoMessage = runCatching {
        reader.read(ProtoMessage.newBuilder(), decoder)
            .apply { if (rawMessage.hasParentEventId()) this.parentEventId = rawMessage.parentEventId }
            .setMetadata(
                rawMessage.toMessageMetadataBuilder(listOf(AvroCodecFactory.PROTOCOL))
                    .setMessageType(reader.schema.name)
            ).build()
    }.getOrElse {
        throw DecodeException(
            "Can't parse message data: ${DatatypeConverter.printHexBinary(bytes)} by schema id: $id",
            it
        )
    }

    protected fun getDecodedData(
        reader: TransportMessageDatumReader,
        decoder: Decoder,
        rawMessage: RawMessage,
        bytes: ByteArray?,
        id: Any
    ): ParsedMessage = runCatching {
        ParsedMessage.builder().apply {
            setId(rawMessage.id)
            rawMessage.eventId?.let(this::setEventId)
            setMetadata(rawMessage.metadata)
            setProtocol(AvroCodecFactory.PROTOCOL)
            setType(reader.schema.name)
            setBody(reader.read(hashMapOf(), decoder))
        }.build()
    }.getOrElse {
        throw DecodeException(
            "Can't parse message data: ${DatatypeConverter.printHexBinary(bytes)} by schema id: $id",
            it
        )
    }

    protected fun getEncodedData(
        writer: MessageDatumWriter,
        parsedMessage: ProtoMessage,
        byteBuf: ByteBuf
    ): ByteString {
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

    protected fun getEncodedData(
        writer: TransportMessageDatumWriter,
        parsedMessage: ParsedMessage,
        byteBuf: ByteBuf
    ): ByteArray {
        val byteArrayOutputStream = ByteArrayOutputStream()
        val encoder: Encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null)
        try {
            writer.write(parsedMessage.body, encoder)
        } catch (e: IOException) {
            throw IllegalStateException("Can't parse message data: ${parsedMessage.toJson()}", e)
        }
        encoder.flush()

        val header = byteBuf.toByteArray()
        return header + byteArrayOutputStream.toByteArray()
    }
}