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

import com.facebook.presto.spi.ColumnHandle
import com.facebook.presto.spi.ColumnMetadata
import com.facebook.presto.spi.RecordCursor
import com.facebook.presto.spi.RecordSet
import com.facebook.presto.spi.predicate.TupleDomain
import com.facebook.presto.spi.type.Type
import com.victoriametrics.presto.model.VmColumnHandle
import com.victoriametrics.presto.model.VmSchema
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.IOException

class VmRecordSet(
        private val constraint: TupleDomain<ColumnHandle>,
        private val queryBuilder: QueryBuilder,
        private val httpClient: OkHttpClient,
        private val columns: List<VmColumnHandle>
) : RecordSet {
    companion object {
        private val columnsByName = VmSchema.columns
                .map { it.name to it }
                .toMap()
    }

    /** Same as [columns], but in the same order as [columnHandles] */
    private val fieldColumns: List<ColumnMetadata> = columns
            .map { columnsByName[it.columnName] ?: error("No field ${it.columnName}") }

    override fun getColumnTypes(): List<Type> {
        return fieldColumns.map { it.type }
    }

    override fun cursor(): RecordCursor {
        val urls = queryBuilder.build(constraint)

        val sources = urls.map { url ->
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

