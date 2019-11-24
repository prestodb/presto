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
import com.facebook.presto.spi.predicate.Marker.Bound.EXACTLY
import com.facebook.presto.spi.predicate.Range
import com.facebook.presto.spi.predicate.SortedRangeSet
import com.facebook.presto.spi.predicate.TupleDomain
import com.facebook.presto.spi.predicate.ValueSet
import com.victoriametrics.presto.model.VmColumnHandle
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl

class QueryBuilder(private val endpoints: List<HttpUrl> = listOf("http://localhost:8428/api/v1".toHttpUrl())) {

    fun build(constraint: TupleDomain<ColumnHandle>): List<HttpUrl> {
        val endpoint = endpoints.random()

        val query = endpoint.newBuilder()
            .addPathSegment("export")
            .addQueryParameter("match", getMatch())
            .build()

        val timestampRanges = getTimestampRanges(constraint)
        if (timestampRanges.isEmpty()) {
            return listOf(query)
        }

        return timestampRanges.map {
            val queryParams = toQueryParams(it)
            val queryBuilder = query.newBuilder()
            queryParams.forEach { (key, value) ->
                queryBuilder.addQueryParameter(key, value)
            }
            queryBuilder.build()
        }
    }

    private fun toQueryParams(range: Range): MutableMap<String, String> {
        val queryParams = mutableMapOf<String, String>()
        val low = range.low
        val high = range.high
        if (low.valueBlock.isPresent) {
            var value = (low.value as Long).toDouble()
            if (low.bound != EXACTLY)
                value++
            value /= 1000
            queryParams["start"] = "%.${3}f".format(value)
        }

        if (high.valueBlock.isPresent) {
            var value = (high.value as Long).toDouble()
            if (high.bound != EXACTLY)
                value--
            value /= 1000
            queryParams["end"] = "%.${3}f".format(value)
        }

        return queryParams
    }

    private fun getMatch() = "{__name__=~\"vm_blocks\"}"

    // TODO: return all ranges rather than "span" and make separate /export calls for each range.
    // TODO: check that presto engine still filters out values that are within the span (per unenforcedConstraints)
    private fun getTimestampRanges(constraint: TupleDomain<ColumnHandle>): List<Range> {
        val ranges = mutableListOf<Range>()

        if (!constraint.columnDomains.isPresent) return ranges

        for (columnDomain in constraint.columnDomains.get()) {
            val column = columnDomain.column as VmColumnHandle
            val values: ValueSet = columnDomain.domain.values

            if (column.columnName != "timestamp") continue
            if (values !is SortedRangeSet) continue

            ranges.addAll(values.orderedRanges)
        }

        return ranges
    }
}
