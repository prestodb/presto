/**
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
package com.victoriametrics.presto.model

import kotlinx.serialization.Decoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.Serializer
import kotlinx.serialization.json.*

@Serializable
data class QueryRangeResponse(
    val status: String,
    val data: Data
) {
    companion object {
        private val json = Json(JsonConfiguration.Stable)
        private val serializer = serializer()

        fun deserialize(content: String): QueryRangeResponse {
            return json.parse(serializer, content)
        }
    }

    @Serializable
    data class Data(
        val resultType: String,
        val result: List<QueryRangeLine>
    )

    @Serializable
    data class QueryRangeLine(
        val metric: Map<String, String>,
        val values: List<Value>
    )

    @Serializable(with = ValueSerializer::class)
    data class Value(val timestamp: Long, val value: String)

    @Serializer(forClass = Value::class)
    object ValueSerializer : KSerializer<Value> {
        override fun deserialize(decoder: Decoder): Value {
            val array = (decoder as JsonInput).decodeJson() as JsonArray
            return Value(array[0].long, array[1].content)
        }
    }
}
