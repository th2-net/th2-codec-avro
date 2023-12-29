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

import com.exactpro.th2.codec.api.*
import com.google.auto.service.AutoService
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser


@AutoService(IPipelineCodecFactory::class)
class AvroCodecFactory : IPipelineCodecFactory {
    private lateinit var codecContext: IPipelineCodecContext

    override val protocols: Set<String>
        get() = PROTOCOLS

    override val settingsClass: Class<out IPipelineCodecSettings> =
        AvroCodecSettings::class.java

    override fun init(pipelineCodecContext: IPipelineCodecContext) {
        this.codecContext = pipelineCodecContext
    }

    override fun create(settings: IPipelineCodecSettings?): IPipelineCodec {
        check(::codecContext.isInitialized) { "'codecContext' was not loaded" }
        val codecSettings = requireNotNull(settings as? AvroCodecSettings) {
            "settings is not an instance of ${AvroCodecSettings::class.java}: ${settings?.let { it::class.java }}"
        }
        val avroMessageIdToDictionaryAlias = codecSettings.avroMessageIdToDictionaryAlias
        val sessionAliasToDictionaryAlias = codecSettings.sessionAliasToDictionaryAlias
        check(
            avroMessageIdToDictionaryAlias.isEmpty()
                .xor(sessionAliasToDictionaryAlias.isEmpty())
        ) { "One of parameters avroMessageIdToDictionaryAlias and sessionAliasToDictionaryAlias can not be empty" }

        return if (avroMessageIdToDictionaryAlias.isEmpty()) {
            AliasAvroCodec(sessionAliasToDictionaryAlias.mapValues { loadSchema(it.value) }, codecSettings)
        } else {
            StandardAvroCodec(avroMessageIdToDictionaryAlias.mapValues { loadSchema(it.value) }, codecSettings)
        }
    }

    private fun loadSchema(schemaAlias: DictionaryAlias): Schema {
        return codecContext[schemaAlias].use(Parser()::parse)
    }

    companion object {
        const val PROTOCOL = "AVRO"
        private val PROTOCOLS = setOf(PROTOCOL)
    }
}