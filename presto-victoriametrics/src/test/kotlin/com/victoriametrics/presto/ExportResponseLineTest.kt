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
import com.victoriametrics.presto.model.ExportResponseLine
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test
import java.nio.charset.StandardCharsets

internal class ExportResponseLineTest {
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

                    assertThat(response.metric.name).isEqualTo("vm_blocks")
                    assertThat(response.values.size).isEqualTo(6)
                    assertThat(response.timestamps.size).isEqualTo(6)
                }
    }


}
