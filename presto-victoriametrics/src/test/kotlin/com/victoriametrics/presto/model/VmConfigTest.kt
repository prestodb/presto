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

import okhttp3.HttpUrl.Companion.toHttpUrl
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test

class VmConfigTest {
    @Test
    fun multiple() {
        val config = mapOf(VmConfig.nodesParamName to "example.com,example2.com")
        val unit = VmConfig(config)
        assertThat(unit.httpUrls[0]).isEqualTo("http://example.com/api/v1/".toHttpUrl())
        assertThat(unit.httpUrls[1]).isEqualTo("http://example2.com/api/v1/".toHttpUrl())
    }

    @Test
    fun withScheme() {
        val config = mapOf(VmConfig.nodesParamName to "http://example.com")
        val unit = VmConfig(config)
        assertThat(unit.httpUrls[0]).isEqualTo("http://example.com/api/v1/".toHttpUrl())
    }

    @Test
    fun withPath() {
        val config = mapOf(VmConfig.nodesParamName to "example.com/foobar")
        val unit = VmConfig(config)
        assertThat(unit.httpUrls[0]).isEqualTo("http://example.com/foobar".toHttpUrl())
    }

    @Test
    fun withPathEmpty() {
        val config = mapOf(VmConfig.nodesParamName to "example.com/")
        val unit = VmConfig(config)
        assertThat(unit.httpUrls[0]).isEqualTo("http://example.com/".toHttpUrl())
    }

    @Test
    fun withSpaces() {
        val config = mapOf(VmConfig.nodesParamName to " example.com , example2.com ")
        val unit = VmConfig(config)
        assertThat(unit.httpUrls[0]).isEqualTo("http://example.com/api/v1/".toHttpUrl())
        assertThat(unit.httpUrls[1]).isEqualTo("http://example2.com/api/v1/".toHttpUrl())
    }

    @Test
    fun withSchemeHttps() {
        val config = mapOf(VmConfig.nodesParamName to "https://example.com")
        val unit = VmConfig(config)
        assertThat(unit.httpUrls[0]).isEqualTo("https://example.com/api/v1/".toHttpUrl())
    }
}
