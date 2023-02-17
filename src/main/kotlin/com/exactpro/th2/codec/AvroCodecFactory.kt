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
    private lateinit var schemaIdToSchema: Map<Int, Schema>


    @Deprecated("Please migrate to the protocols property")
    override val protocol: String = PROTOCOL

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

        val idToSchema = codecSettings.messageIdsToSchemaAliases
        if (idToSchema.isEmpty()) {
            throw IllegalArgumentException("Parameter messageIdsToSchemaAliases is required")
        }
        try {
            val map = idToSchema.split(SCHEMA_DELIMITER).associate {
                val (key, value) = it.split(SCHEMA_KEY_VALUE_DELIMITER)
                key.trim().toInt() to value.trim()
            }
            schemaIdToSchema = map.mapValues { loadSchema(it.value) }
        } catch (e: Exception) {
            throw IllegalArgumentException("Parse error messageIdsToSchemaAliases: $idToSchema", e)
        }
        return AvroCodec(schemaIdToSchema, codecSettings)
    }

    private fun loadSchema(schemaAlias: DictionaryAlias): Schema {
        return codecContext[schemaAlias].use(Parser()::parse)
    }

    companion object {
        const val PROTOCOL = "AVRO"
        const val SCHEMA_DELIMITER = ","
        const val SCHEMA_KEY_VALUE_DELIMITER = ":"
    }
}