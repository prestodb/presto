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
package com.victoriametrics.presto

import com.facebook.airlift.log.Logger
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.victoriametrics.presto.model.ExportResponseLine
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test
import java.io.IOException
import java.nio.charset.StandardCharsets

class ExportResponseLineTest {
    private val log = Logger.get(this::class.java)!!
    /**
     * `http://0.0.0.0:8428/api/v1/export?match={__name__=~"vm_blocks"}`
     */
    @Test
    fun testRead() {
        val resource = javaClass.getResource("export.response.ndjson")!!
        val content = resource.readText(StandardCharsets.UTF_8)
        content.lines()
                .filter { it.isNotEmpty() }
                .forEachIndexed { i, line ->
                    log.debug("Parsing line {}", i)
                    val response = ExportResponseLine.deserialize(line)

                    // assertThat(response.metricfullName).isEqualTo("""{"__name__":"vm_blocks","job":"vm","type":"indexdb","instance":"victoriametrics:8428"}""")
                    assertThat(response.values.size).isEqualTo(6)
                    assertThat(response.timestamps.size).isEqualTo(6)
                }
    }

    @Test
    fun testParse() {
        val resource = javaClass.getResource("export.response.ndjson")!!
        val content = resource.readText(StandardCharsets.UTF_8)
        val json: String = content.lines()[0]
        val mapper = ObjectMapper()
        val read: ExportResponseLine3 = mapper.readValue(json, ExportResponseLine3::class.java)
    }

    data class ExportResponseLine3(
            val metric: Metric3,
            val values: DoubleArray,
            val timestamps: LongArray
    )

    class Metric3Deserializer @JvmOverloads constructor(vc: Class<*>? = null) : StdDeserializer<Metric3>(vc) {
        @Throws(IOException::class, JsonProcessingException::class)
        override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): Metric3 {
            // val node: JsonNode = jp.codec.readTree(jp)
            jp.currentToken.isStructStart
            assert(jp.isExpectedStartObjectToken) { jp.currentToken }
            jp.nextToken()
            val fullName = String(jp.currentToken.asCharArray())
            // val id = (node["id"] as IntNode).numberValue() as Int
            // val itemName = node["itemName"].asText()
            // val userId = (node["createdBy"] as IntNode).numberValue() as Int
            return Metric3(fullName)
        }
    }


    @JsonDeserialize(using = Metric3Deserializer::class)
    data class Metric3(val fullName: String)
}
