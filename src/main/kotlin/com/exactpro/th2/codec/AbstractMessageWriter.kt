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

import org.apache.avro.Conversions
import org.apache.avro.Schema
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

abstract class AbstractMessageWriter<T>(schema: Schema, private val enableIdPrefixEnumFields: Boolean = false)
    : GenericDatumWriter<T>(schema, getData()) {

    companion object {
        fun getData(): GenericData? {
            return GenericData.get().apply {
                addLogicalTypeConversion(Conversions.DecimalConversion())
                addLogicalTypeConversion(TimeConversions.DateConversion())
                addLogicalTypeConversion(TimeConversions.TimeMillisConversion())
                addLogicalTypeConversion(TimeConversions.TimeMicrosConversion())
                addLogicalTypeConversion(TimeConversions.TimestampMillisConversion())
                addLogicalTypeConversion(TimeConversions.TimestampMicrosConversion())
                addLogicalTypeConversion(TimeConversions.LocalTimestampMillisConversion())
                addLogicalTypeConversion(TimeConversions.LocalTimestampMicrosConversion())
            }
        }

        enum class Type(val type: String) {
            DECIMAL("decimal"),
            DATE("date"),
            TIME_MILLIS("time-millis"),
            TIME_MICROS("time-micros"),
            TIMESTAMP_MILLIS("timestamp-millis"),
            TIMESTAMP_MICROS("timestamp-micros"),
            LOCAL_TIMESTAMP_MILLIS("local-timestamp-millis"),
            LOCAL_TIMESTAMP_MICROS("local-timestamp-micros")
        }

        val localTimeWithMillisConverter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC)
        val localTimeWithMicrosConverter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC)
        val localDateTimeWithMillisConverter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneOffset.UTC)
        val localDateTimeWithMicrosConverter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC)

        const val FORMAT_TYPE_ERROR = "Unsupported type %s for %s"
        const val UNION_FIELD_NAME_TYPE_DELIMITER = '-'
        const val UNION_ID_PREFIX = "Id"
    }
}