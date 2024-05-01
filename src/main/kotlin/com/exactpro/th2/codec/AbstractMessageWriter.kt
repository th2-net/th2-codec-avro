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

import org.apache.avro.AvroTypeException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

abstract class AbstractMessageWriter<T>(schema: Schema, private val enableIdPrefixEnumFields: Boolean)
    : GenericDatumWriter<T>(schema, getData()) {

    protected fun <T> resolveUnion(union: Schema, fieldName: String?, enumValue: T?): Int {
        if (enableIdPrefixEnumFields.and(enumValue != null)) {
            try {
                return checkNotNull(
                    fieldName?.substringBefore(UNION_FIELD_NAME_TYPE_DELIMITER)?.substringAfter(UNION_ID_PREFIX)
                        ?.toInt()
                ) { "Schema id not found in field name: $fieldName" }
            } catch (e: NumberFormatException) {
                throw AvroTypeException(
                    "Union prefix: $UNION_ID_PREFIX'{schema id}'$UNION_FIELD_NAME_TYPE_DELIMITER not found in field name: $fieldName",
                    e
                )
            }
        }
        val schemaName = checkNotNull(
            if (enumValue == null) Schema.Type.NULL.getName() else fieldName?.substringBefore(
                UNION_FIELD_NAME_TYPE_DELIMITER
            )
        ) { "Union prefix: {avro type}$UNION_FIELD_NAME_TYPE_DELIMITER not found in field name: $fieldName" }
        return checkNotNull(union.getIndexNamed(schemaName)) { "Schema with name: $schemaName not found in union parent schema: ${union.name}" }
    }

    protected fun convertFieldValue(value: String, typeName: String): Any {
        val converter = typeNameToConverter[typeName] ?: throw AvroTypeException("Logical type $typeName is not supported}")
        return converter(value)
    }

    companion object {
        private val typeNameToConverter = buildMap<String, (String) -> Any> {
            put("decimal") { it.toBigDecimal() }
            put("date") { LocalDate.parse(it) }
            put("time-millis") { java.time.LocalTime.parse(it, localTimeWithMillisConverter) }
            put("time-micros") { java.time.LocalTime.parse(it, localTimeWithMicrosConverter) }
            put("timestamp-millis") { Instant.parse(it) }
            put("timestamp-micros") { Instant.parse(it) }
            put("local-timestamp-millis") { LocalDateTime.parse(it, localDateTimeWithMillisConverter) }
            put("local-timestamp-micros") { LocalDateTime.parse(it, localDateTimeWithMicrosConverter) }
        }

        private val localTimeWithMillisConverter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC)
        private val localTimeWithMicrosConverter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC)
        private val localDateTimeWithMillisConverter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneOffset.UTC)
        private val localDateTimeWithMicrosConverter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC)

        const val FORMAT_TYPE_ERROR = "Unsupported type %s for %s"
        const val UNION_FIELD_NAME_TYPE_DELIMITER = '-'
        const val UNION_ID_PREFIX = "Id"
    }
}