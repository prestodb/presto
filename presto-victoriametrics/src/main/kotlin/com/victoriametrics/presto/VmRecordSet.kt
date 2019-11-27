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

import com.facebook.presto.spi.ColumnMetadata
import com.facebook.presto.spi.RecordCursor
import com.facebook.presto.spi.RecordSet
import com.facebook.presto.spi.type.Type
import com.victoriametrics.presto.model.VmColumnHandle
import com.victoriametrics.presto.model.VmSplit
import okhttp3.Call
import okhttp3.Request
import java.io.IOException
import java.util.concurrent.CompletableFuture

class VmRecordSet(
        metadata: VmMetadata,
        private val httpClient: Call.Factory,
        queryBuilder: QueryBuilder,
        split: VmSplit,
        requestedColumns: List<VmColumnHandle>
) : RecordSet {

    /** In the same order as `requestedColumns` */
    private val fieldColumns: List<ColumnMetadata> = requestedColumns
            .map { metadata.columnsByName[it.columnName] ?: error("No field ${it.columnName}") }

    private val urls = queryBuilder.build(split.constraint)

    override fun getColumnTypes(): List<Type> {
        return fieldColumns.map { it.type }
    }

    override fun cursor(): RecordCursor {
        // TODO: parallelize requests
        val sources = urls
                .map { url ->
                    CompletableFuture.runAsync {  }
            val request = Request.Builder()
                    .get()
                    .url(url)
                    .build()

            val call = httpClient.newCall(request)
            val response = call.execute()

            if (response.code / 100 != 2) {
                throw IOException("Response code is ${response.code}")
            }

            response.body!!.source()
        }
        return VmRecordCursor(sources, fieldColumns)
    }
}

