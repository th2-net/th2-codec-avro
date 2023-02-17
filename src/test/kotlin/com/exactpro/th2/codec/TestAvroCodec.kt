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
import org.apache.commons.codec.binary.Hex
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.File
import javax.xml.bind.DatatypeConverter
import kotlin.test.assertEquals

class TestAvroCodec {
    private val schemaIdToSchema = mapOf(
        1 to getSchema("big_schema.avsc"),
        2 to getSchema("big_schema_without_union.avsc"),
        3 to getSchema("schema_logical_types.avsc"),
        4 to getSchema("union_schema_with_logical_types.avsc")
    )
    private var codec: AvroCodec = AvroCodec(schemaIdToSchema, AvroCodecSettings())

    @Test
    fun `test full decode encode`() {
        val rawBytes =
            DatatypeConverter.parseHexBinary(
                "00000000010001000001B3AAAFFC030002D190BEF38A98E0C01C029AB9EDDF8CCDDFA1B40100ADEAA4E8EFC8F8ED0BE0FB083E027CB77F3F0080AC373E388777341CC7B43F024C3A68BEBB13C03F0053D30639AA9BEA3F143F34DA30EBF67E46854F0002046F720218746668676E706664616D7265001C6A627867647472707675726D7068000000000000020246676E7466677365746D7575656163766D627678697473626D707979717063636D64657900020A1E66666366716C6C6C676F64776865631267717177706B7463711870697367676B776B6E6D6D6E00127661756C736F64716B00020A4867726B67666B626467676F666B67657373756F776C69646B6365726B6667646177686D680E726C6F63737371286D63737378656F6865616C6C79657269767477761A796E76757575616566616B6668166763626B77676A796E77640A746C746C6D4C6F6E716B716A61636866626279786C6879677276696F6675756A716C706C686667636B6A6C790E6E6468786D77711472637976746A6B636A73486C6375796A65727572616D75737278706C7869636772676C6F7967756A61646761616A670000000640686164746D766A70636F71706573677870736C737766706271726A757963756C16757076796D6C6A6A6476640A6C6E6B796B0467650A676F7864742E6F6D686C76787070686E6F79716171777771636569746600160226020202F48281810E02009FF4B6D3A2B985D0B501001870233F02EFA688C70D02CC94D9F7080293F89FDD0E000002C5A7FFAE050286DA90C30C000A14686A746B79626F626F642A6A627176616170686476637763746C6A6A6D65736B266E6B6568627371736D76696D756B646E63737034617071667979726C747264676B757972636677767367696270654A67696A696868646A66666F756A666567676C796D766D797571716469616C79656F64786471166E6477626E7478726F6C6E42796A79686F6D756279746D6C6772616E656C70666561686B66796C6E6B70716C68306B7173626F767970676F6270757465627172656B6B796B61206E7062647061786D66726D67756C79660E7569667767676F0002E7ACC49E03"
            )
        decodeToEncode(rawBytes, 37)
    }
    @Test
    fun `test full decode encode union id prefix`() {
        codec = AvroCodec(schemaIdToSchema, AvroCodecSettings().also { it.enableIdPrefixEnumFields = true })
        val rawBytes =
            DatatypeConverter.parseHexBinary(
                "00000000010001000001B3AAAFFC030002D190BEF38A98E0C01C029AB9EDDF8CCDDFA1B40100ADEAA4E8EFC8F8ED0BE0FB083E027CB77F3F0080AC373E388777341CC7B43F024C3A68BEBB13C03F0053D30639AA9BEA3F143F34DA30EBF67E46854F0002046F720218746668676E706664616D7265001C6A627867647472707675726D7068000000000000020246676E7466677365746D7575656163766D627678697473626D707979717063636D64657900020A1E66666366716C6C6C676F64776865631267717177706B7463711870697367676B776B6E6D6D6E00127661756C736F64716B00020A4867726B67666B626467676F666B67657373756F776C69646B6365726B6667646177686D680E726C6F63737371286D63737378656F6865616C6C79657269767477761A796E76757575616566616B6668166763626B77676A796E77640A746C746C6D4C6F6E716B716A61636866626279786C6879677276696F6675756A716C706C686667636B6A6C790E6E6468786D77711472637976746A6B636A73486C6375796A65727572616D75737278706C7869636772676C6F7967756A61646761616A670000000640686164746D766A70636F71706573677870736C737766706271726A757963756C16757076796D6C6A6A6476640A6C6E6B796B0467650A676F7864742E6F6D686C76787070686E6F79716171777771636569746600160226020202F48281810E02009FF4B6D3A2B985D0B501001870233F02EFA688C70D02CC94D9F7080293F89FDD0E000002C5A7FFAE050286DA90C30C000A14686A746B79626F626F642A6A627176616170686476637763746C6A6A6D65736B266E6B6568627371736D76696D756B646E63737034617071667979726C747264676B757972636677767367696270654A67696A696868646A66666F756A666567676C796D766D797571716469616C79656F64786471166E6477626E7478726F6C6E42796A79686F6D756279746D6C6772616E656C70666561686B66796C6E6B70716C68306B7173626F767970676F6270757465627172656B6B796B61206E7062647061786D66726D67756C79660E7569667767676F0002E7ACC49E03"
            )
        decodeToEncode(rawBytes, 37)
    }

    @Test
    fun `test decode encode`() {
        val rawBytes =
            DatatypeConverter.parseHexBinary(
                "0000000002000012B7ADB9A75E63FF6149CAFFDA9A0DF7C3FC8CDFF08FED78EF3369D249F1EE3F3E66676F71776D75686762676D6E636A78797679786F67666E636A746E646C6600020A326F686B786B6B686C6D616671776C716162747561696F6E716E0E6278796D7269792069626D7167766B697476676662666B644A62646168726F657964676B6F74676A766275646862757279657374716261716F766B626474267466776D6278676D6977647163786F6179777500044E6770636D6A796C777061736D71667562677377656B7671696169776A7965616F676A726A7962610C7978666E6D78246E6267776D686E69687265786A776E6E75631E68726461726771686179646A6A6471002E"
            )
        decodeToEncode(rawBytes, 12)
    }

    @Test
    fun `test decode encode logical types`() {
        val rawBytes =
            DatatypeConverter.parseHexBinary(
                "000000000314303132333435363738390403E888A30292F3BE03D2C4B3A31BE0BEB4CDF65D809E8AE2CCE6DD0592B3B2B7D75DD2C4DFE188F3DB05"
            )
        decodeToEncode(rawBytes, 9)
    }

    @Test
    fun `test decode encode union with logical types`() {
        val rawBytes =
            DatatypeConverter.parseHexBinary(
                "000000000402F586B9CF0E"
            )
        decodeToEncode(rawBytes, 1)
    }

    private fun decodeToEncode(rawBytes: ByteArray?, expected: Int) {
        val body = ByteString.copyFrom(rawBytes)
        val decodeGroup = decode(body)
        val actualCountFields = decodeGroup.messagesList[0].message.fieldsMap.size
        assertEquals(expected, actualCountFields)
        val encodeBody = encode(decodeGroup)
        assertEquals(body, encodeBody)
    }

    private fun decode(body: ByteString?): MessageGroup {
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
        assertEquals(1, decodeMessages.size)
        return decodeGroup
    }

    private fun encode(messageGroup: MessageGroup): ByteString? {
        val encodeMessages = codec.encode(messageGroup).messagesList
        assertEquals(1, encodeMessages.size)
        return encodeMessages[0].rawMessage.body
    }

    private fun getSchema(name: String): Schema =
        this::class.java.classLoader.getResourceAsStream("schemas${File.separatorChar}${name}")
            .use(Parser()::parse)

    @Disabled
    @Test
    fun generateAvroRandomDataBySchema() {
        val schema = schemaIdToSchema[4]
        val writer = GenericDatumWriter<Any>()
        writer.setSchema(schema)
        val outputStream = ByteArrayOutputStream(8192)
        val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null)
        for (datum in RandomData(schema, 1)) {
            writer.write(datum, encoder)
        }
        encoder.flush()
        val data = outputStream.toByteArray()
        println(Hex.encodeHexString(data))
    }
}