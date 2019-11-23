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

import kotlinx.serialization.Required
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration

@Serializable
data class ExportResponseLine(
    @Required val metric: Metric,
    @Required val values: DoubleArray,
    @Required val timestamps: LongArray
) {
    companion object {
        private val json = Json(JsonConfiguration.Stable)
        private val serializer = serializer()

        fun deserialize(content: String): ExportResponseLine {
            return json.parse(serializer, content)
        }
    }

    init {
        require(timestamps.size == values.size) { "Sizes aren't equal: ${timestamps.size} vs ${values.size}" }
    }

    @Serializable
    data class Metric(
        @SerialName("__name__")
        val name: String,
        val job: String,
        val type: String,
        val instance: String
    ) {
        override fun toString(): String {
            // TODO: return original string from export, don't parse its JSON
            return """{"__name__":"$name","job":"$job","type":"$type","instance":"$instance"}"""
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ExportResponseLine

        if (metric != other.metric) return false

        return true
    }

    override fun hashCode(): Int {
        return metric.hashCode()
    }

    override fun toString(): String {
        return "ExportResponseLine(metric=$metric, values.size=${values.size}, timestamps.size=${timestamps.size})"
    }
}
