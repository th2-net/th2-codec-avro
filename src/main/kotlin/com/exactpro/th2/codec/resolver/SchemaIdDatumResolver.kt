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

class SchemaIdDatumResolver(
    schemaIdToSchema: Map<Int, Schema>,
    enableIdPrefixEnumFields: Boolean = false
) : IDatumResolver<Int> {
    private val datumReaders = schemaIdToSchema.mapValues {
        MessageDatumReader(
            it.value,
            enableIdPrefixEnumFields
        )
    }
    private val datumWriters = schemaIdToSchema.mapValues {
        MessageDatumWriter(
            it.value,
            enableIdPrefixEnumFields
        )
    }
    private val schemaIdToMessageName = checkSchemaNames(schemaIdToSchema.mapValues { it.value.name })
    private val messageNameToSchemaId = schemaIdToMessageName.entries.associate { (key, value) -> value to key }


    override fun getReader(value: Int): MessageDatumReader {
        return checkNotNull(datumReaders[value]) { "No reader found for schema id: $value" }
    }

    override fun getWriter(value: Int): MessageDatumWriter {
        return checkNotNull(datumWriters[value]) { "No writer found for schema id: $value" }
    }

    fun getSchemaId(messageName: String): Int {
        return checkNotNull(messageNameToSchemaId[messageName]) { "No schema id found for message type: $messageName" }
    }

    private fun checkSchemaNames(map: Map<Int, String>): Map<Int, String> {
        val duplicates = findDuplicates(map.values)
        check(duplicates.isEmpty()) {
            "Root element names in schemas must be unique. Duplicate names: ${
                duplicates.joinToString(
                    " ,"
                )
            }. This name is name of decoded message. It is not possible to resolve schema when encoding."
        }
        return map
    }

    private fun findDuplicates(list: Collection<String>): Set<String> {
        return list.filter { element -> list.count { it == element } > 1 }.toSet()
    }
}