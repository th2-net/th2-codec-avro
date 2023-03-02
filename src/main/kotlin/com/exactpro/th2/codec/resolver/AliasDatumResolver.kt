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
package com.exactpro.th2.codec.resolver

import com.exactpro.th2.codec.MessageDatumReader
import com.exactpro.th2.codec.MessageDatumWriter
import org.apache.avro.Schema
import org.apache.commons.io.FilenameUtils

class AliasDatumResolver(
    aliasToSchema: Map<String, Schema>,
    enableIdPrefixEnumFields: Boolean = false
) : IDatumResolver<String> {
    private val datumReadersCache: MutableMap<String, MessageDatumReader> = mutableMapOf()
    private val datumWritersCache: MutableMap<String, MessageDatumWriter> = mutableMapOf()
    private var wildcardAliases: Array<Alias>

    init {
        val (wildcardAliases, simpleAliases) = aliasToSchema.toList().map {
            Alias(
                it.first,
                MessageDatumReader(it.second, enableIdPrefixEnumFields),
                MessageDatumWriter(it.second, enableIdPrefixEnumFields)
            )
        }.partition { isWildcard(it.wildcardAlias) }

        simpleAliases.forEach { aliasElement ->
            val alias = aliasElement.wildcardAlias
            datumReadersCache[alias] = aliasElement.reader
            datumWritersCache[alias] = aliasElement.writer
        }
        this.wildcardAliases = wildcardAliases.toTypedArray()
    }

    data class Alias(
        val wildcardAlias: String,
        val reader: MessageDatumReader,
        val writer: MessageDatumWriter
    )

    override fun getReader(value: String): MessageDatumReader {
        return checkNotNull(
            datumReadersCache[value] ?: resolveReadAlias(value)
        ) { "No reader found for session alias: $value" }
    }

    override fun getWriter(value: String): MessageDatumWriter {
        return checkNotNull(
            datumWritersCache[value] ?: resolveWriteAlias(value)
        ) { "No writer found for session alias: $value" }
    }

    private fun resolveReadAlias(alias: String): MessageDatumReader? {
        return resolveAlias(alias)?.reader
    }

    private fun resolveWriteAlias(alias: String): MessageDatumWriter? {
        return resolveAlias(alias)?.writer
    }

    private fun resolveAlias(alias: String): Alias? {
        wildcardAliases.forEach { aliasElement ->
            if (FilenameUtils.wildcardMatch(alias, aliasElement.wildcardAlias)) {
                datumReadersCache[alias] = aliasElement.reader
                datumWritersCache[alias] = aliasElement.writer
                return aliasElement
            }
        }
        return null
    }

    private fun isWildcard(value: String): Boolean {
        return (value.indexOf('?') != -1).or(value.indexOf('*') != -1)
    }

}