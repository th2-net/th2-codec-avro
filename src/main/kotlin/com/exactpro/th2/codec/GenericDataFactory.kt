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
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.GenericData

fun getData(): GenericData = GenericData.get().apply {
        addLogicalTypeConversion(Conversions.DecimalConversion())
        addLogicalTypeConversion(TimeConversions.DateConversion())
        addLogicalTypeConversion(TimeConversions.TimeMillisConversion())
        addLogicalTypeConversion(TimeConversions.TimeMicrosConversion())
        addLogicalTypeConversion(TimeConversions.TimestampMillisConversion())
        addLogicalTypeConversion(TimeConversions.TimestampMicrosConversion())
        addLogicalTypeConversion(TimeConversions.LocalTimestampMillisConversion())
        addLogicalTypeConversion(TimeConversions.LocalTimestampMicrosConversion())
    }