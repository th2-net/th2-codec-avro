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
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

class AliasDatumResolver(
    aliasToSchema: Map<String, Schema>,
    enableIdPrefixEnumFields: Boolean = false
) : IDatumResolver<String> {
    private val datumCache: MutableMap<String, DatumPair> = mutableMapOf()
    private val wildcardAliases: List<Alias>
    private val lock: Lock = ReentrantLock()

    init {
        val (wildcardAliases, simpleAliases) = aliasToSchema.toList().map {
            Alias(
                it.first,
                DatumPair(
                    MessageDatumReader(it.second, enableIdPrefixEnumFields),
                    MessageDatumWriter(it.second, enableIdPrefixEnumFields)
                )
            )
        }.partition { isWildcard(it.wildcardAlias) }

        simpleAliases.forEach { aliasElement ->
            val alias = aliasElement.wildcardAlias
            datumCache[alias] = aliasElement.datums
        }
        this.wildcardAliases = wildcardAliases
    }

    data class Alias(
        val wildcardAlias: String,
        val datums: DatumPair
    )

    data class DatumPair(
        val reader: MessageDatumReader,
        val writer: MessageDatumWriter
    )

    override fun getReader(value: String): MessageDatumReader {
        return getDatums(value)?.reader ?: throw IllegalStateException("No reader found for session alias: $value")
    }

    override fun getWriter(value: String): MessageDatumWriter {
        return getDatums(value)?.writer ?: throw IllegalStateException("No writer found for session alias: $value")
    }

    private fun getDatums(value: String): DatumPair? {
        lock.lock()
        try {
            return datumCache[value] ?: resolveAlias(value)
        } finally {
            lock.unlock()
        }
    }


    private fun resolveAlias(alias: String): DatumPair? {
        wildcardAliases.forEach { aliasElement ->
            if (FilenameUtils.wildcardMatch(alias, aliasElement.wildcardAlias)) {
                val datums = aliasElement.datums
                datumCache[alias] = datums
                return datums
            }
        }
        return null
    }

    private fun isWildcard(value: String): Boolean {
        return (value.indexOf('?') != -1).or(value.indexOf('*') != -1)
    }

}