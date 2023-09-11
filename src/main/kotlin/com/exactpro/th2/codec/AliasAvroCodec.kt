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

import com.exactpro.th2.codec.resolver.AliasDatumResolver
import com.exactpro.th2.codec.util.toJson
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.google.protobuf.ByteString
import io.netty.buffer.Unpooled
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory

class AliasAvroCodec(sessionAliasToSchema: Map<String, Schema>, settings: AvroCodecSettings) :
    AbstractAvroCodec(settings) {
    private val aliasResolver = AliasDatumResolver(sessionAliasToSchema, enableIdPrefixEnumFields, enablePrefixEnumFieldsDecode)

    override fun decodeRawMessage(rawMessage: ProtoRawMessage, sessionAlias: String): Message {
        check(sessionAlias.isNotBlank()) { "Session alias cannot be empty. Raw message: ${rawMessage.toJson()}" }
        val bytes = rawMessage.body.toByteArray()
        val reader = aliasResolver.getReader(sessionAlias)
        val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        return getDecodedData(reader, decoder, rawMessage, bytes, sessionAlias)
    }

    override fun decodeRawMessage(rawMessage: RawMessage, sessionAlias: String): ParsedMessage {
        check(sessionAlias.isNotBlank()) { "Session alias cannot be empty. Raw message: ${rawMessage.toJson()}" }
        val bytes = rawMessage.body.toByteArray()
        val reader = aliasResolver.getTransportReader(sessionAlias)
        val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        return getDecodedData(reader, decoder, rawMessage, bytes, sessionAlias)
    }

    override fun encodeMessage(parsedMessage: Message, sessionAlias: String): ByteString {
        check(sessionAlias.isNotBlank()) { "Session alias cannot be empty. Parsed message: ${parsedMessage.toJson()}" }
        val byteBuf = Unpooled.buffer()
        val writer = aliasResolver.getWriter(sessionAlias)
        return getEncodedData(writer, parsedMessage, byteBuf)
    }

    override fun encodeMessage(parsedMessage: ParsedMessage, sessionAlias: String): ByteArray {
        check(sessionAlias.isNotBlank()) { "Session alias cannot be empty. Parsed message: ${parsedMessage.toJson()}" }
        val byteBuf = Unpooled.buffer()
        val writer = aliasResolver.getTransportWriter(sessionAlias)
        return getEncodedData(writer, parsedMessage, byteBuf)
    }
}