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
import com.facebook.presto.spi.predicate.Domain
import com.facebook.presto.spi.predicate.Range
import com.facebook.presto.spi.predicate.TupleDomain
import com.facebook.presto.spi.predicate.ValueSet
import com.facebook.presto.spi.type.TimestampType
import com.victoriametrics.presto.inject.TestVmModule
import com.victoriametrics.presto.model.VmColumnHandle
import okhttp3.HttpUrl
import org.assertj.core.api.Assertions.assertThat
import org.testng.annotations.Test

class TestQueryBuilder {
    val unit = TestVmModule.init().queryBuilder()

    /**
     * WHERE timestamp > TIMESTAMP '2019-10-26 21:40:10.646'
     */
    @Test
    fun testGreaterThan() {
        val range = Range.greaterThan(TimestampType.TIMESTAMP, 1572151210000L)
        val url = test(range)[0]

        assertThat(url.queryParameter("start")).isEqualTo("1572151210.001")
        assertThat(url.queryParameter("end")).isNull()
    }

    /**
     * WHERE timestamp >= TIMESTAMP '2019-10-26 21:40:10.646'
     */
    @Test
    fun testGreaterThanOrEqual() {
        val range = Range.greaterThanOrEqual(TimestampType.TIMESTAMP, 1572151210000L)
        val url = test(range)[0]

        assertThat(url.queryParameter("start")).isEqualTo("1572151210.000")
        assertThat(url.queryParameter("end")).isNull()
    }

    /**
     * WHERE timestamp < TIMESTAMP '2019-10-26 21:40:10.646'
     */
    @Test
    fun testLessThan() {
        val range = Range.lessThan(TimestampType.TIMESTAMP, 1572151210000L)
        val url = test(range)[0]

        assertThat(url.queryParameter("start")).isNull()
        assertThat(url.queryParameter("end")).isEqualTo("1572151209.999")
    }

    /**
     * WHERE timestamp <= TIMESTAMP '2019-10-26 21:40:10.646'
     */
    @Test
    fun testLessThanOrEqual() {
        val range = Range.lessThanOrEqual(TimestampType.TIMESTAMP, 1572151210000L)
        val url = test(range)[0]

        assertThat(url.queryParameter("start")).isNull()
        assertThat(url.queryParameter("end")).isEqualTo("1572151210.000")
    }

    /**
     * WHERE timestamp BETWEEN TIMESTAMP '2019-10-26 21:40:10.646' AND TIMESTAMP '2019-10-26 22:40:10.646';
     */
    @Test
    fun testBetween() {
        val range = Range.range(TimestampType.TIMESTAMP, 1572151210646L, true, 1572154810000L, true)
        val url = test(range)[0]

        assertThat(url.queryParameter("start")).isEqualTo("1572151210.646")
        assertThat(url.queryParameter("end")).isEqualTo("1572154810.000")
    }

    /**
     * WHERE timestamp NOT BETWEEN TIMESTAMP '2019-10-26 00:00:00.000' AND TIMESTAMP '2019-10-30 00:00:00.999';
     */
    @Test
    fun testNotBetween() {
        val range1 = Range.lessThan(TimestampType.TIMESTAMP, 1572151210000L)
        val range2 = Range.greaterThan(TimestampType.TIMESTAMP, 1572151210999L)
        val urls = test(range1, range2)

        assertThat(urls[0].queryParameter("start")).isNull()
        assertThat(urls[0].queryParameter("end")).isEqualTo("1572151209.999")
        assertThat(urls[1].queryParameter("start")).isEqualTo("1572151211.000")
        assertThat(urls[1].queryParameter("end")).isNull()
    }

    private fun test(range: Range, vararg ranges: Range): List<HttpUrl> {
        val timestampHandle: ColumnHandle = VmColumnHandle("timestamp")

        val values = ValueSet.ofRanges(range, *ranges)
        val domain: Domain = Domain.create(values, false)

        // val element = TupleDomain.ColumnDomain<ColumnHandle>(
        //     timestampHandle, domain
        // )
        // val listOf: List<TupleDomain.ColumnDomain<ColumnHandle>> = listOf(element)
        val map = mapOf(timestampHandle to domain)
        val constraint = TupleDomain.withColumnDomains(map)

        return unit.build(constraint)
    }

    @Test
    fun testPushdown() {
        // BuiltInFunctionHandle
        // CallExpression("BETWEEN", )
    }
}
