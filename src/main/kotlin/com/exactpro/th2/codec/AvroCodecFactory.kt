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
import com.exactpro.th2.codec.api.IPipelineCodecFactory
import com.exactpro.th2.codec.api.IPipelineCodecSettings
import com.google.auto.service.AutoService
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import java.io.InputStream


@AutoService(IPipelineCodecFactory::class)
class AvroCodecFactory : IPipelineCodecFactory {
    private lateinit var schema: Schema


    @Deprecated("Please migrate to the protocols property")
    override val protocol: String = PROTOCOL

    override val settingsClass: Class<out IPipelineCodecSettings> =
        AvroCodecSettings::class.java

    override fun init(dictionary: InputStream) {
        check(!this::schema.isInitialized) { "Factory already initialized" }
        this.schema = Parser().parse(dictionary)
    }

    override fun create(settings: IPipelineCodecSettings?): IPipelineCodec {
        check(::schema.isInitialized) { "'dictionary' was not loaded" }
        return AvroCodec(schema, requireNotNull(settings as? AvroCodecSettings) {
            "settings is not an instance of ${AvroCodecSettings::class.java}: ${settings?.let { it::class.java }}"
        })
    }

    companion object {
        const val PROTOCOL = "AVRO"
    }
}