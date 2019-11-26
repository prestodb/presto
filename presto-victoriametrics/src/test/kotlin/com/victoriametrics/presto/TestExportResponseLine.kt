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

class TestExportResponseLine {
    private val log = Logger.get(this::class.java)!!
    /**
     * `http://0.0.0.0:8428/api/v1/export?match={__name__=~"vm_blocks"}`
     */
    @Test
    fun testRead() {
        val resource = javaClass.getResource("export.response.ndjson")!!
        val content = resource.readText(StandardCharsets.UTF_8)
        val lines = content.lines()
            .filter { it.isNotEmpty() }
            .toList()

        val response0 = ExportResponseLine.deserialize(lines[0])
        assertThat(response0.metricName).isEqualTo(
            """{"__name__":"vm_blocks","job":"vm","type":"indexdb","instance":"victoriametrics:8428"}"""
        )
        assertThat(response0.values.size).isEqualTo(6)
        assertThat(response0.timestamps.size).isEqualTo(6)

        val response1 = ExportResponseLine.deserialize(lines[1])
        assertThat(response1.metricName).isEqualTo(
            """{"__name__":"vm_blocks","job":"vm","type":"storage/small","instance":"victoriametrics:8428"}"""
        )
        val response2 = ExportResponseLine.deserialize(lines[2])
        assertThat(response2.metricName).isEqualTo(
            """{"__name__":"vm_blocks","job":"vm","type":"storage/big","instance":"victoriametrics:8428"}"""
        )
    }
}
