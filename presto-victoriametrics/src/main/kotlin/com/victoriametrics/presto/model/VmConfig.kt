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
@file:Suppress("ConvertSecondaryConstructorToPrimary", "MemberVisibilityCanBePrivate")

package com.victoriametrics.presto.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl

class VmConfig {
    val map: Map<String, String>
        // For Jackson
        @JsonProperty("map") get

    val httpUrls: List<HttpUrl>

    @JsonCreator
    constructor(@JsonProperty("map") map: Map<String, String>) {
        this.map = map
        this.httpUrls = {
            (map[nodesParamName] ?: error("Config param '$nodesParamName' is required"))
                .split(',')
                .map { toHttpUrl(it) }
        }()
    }

    private fun toHttpUrl(rawStr: String): HttpUrl {
        var str = rawStr.trim()

        if (!(str.startsWith("http://") || str.startsWith("https://"))) {
            str = "$defaultScheme://$str"
        }

        var result = str.toHttpUrl()
        if ((result.pathSegments.isEmpty() || result.pathSegments[0].isEmpty()) && !str.endsWith('/')) {
            result = result.resolve(defaultExportPath)!!
        }
        return result
    }

    companion object {
        internal const val defaultExportPath = "/api/v1/"
        internal const val defaultScheme = "http"
        internal const val nodesParamName = "victoriametrics.vmselect-endpoints"

    }
}
