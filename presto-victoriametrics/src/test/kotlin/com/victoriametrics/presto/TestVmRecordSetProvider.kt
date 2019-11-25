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

import com.facebook.presto.spi.ConnectorSession
import com.facebook.presto.spi.predicate.TupleDomain
import com.facebook.presto.spi.type.DoubleType
import com.facebook.presto.spi.type.TimestampType
import com.facebook.presto.spi.type.VarcharType
import com.victoriametrics.presto.model.VmColumnHandle
import com.victoriametrics.presto.model.VmSplit
import com.victoriametrics.presto.model.VmTransactionHandle
import io.airlift.slice.Slices
import io.mockk.every
import io.mockk.mockk
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.testng.annotations.Test
import java.nio.charset.StandardCharsets


class TestVmRecordSetProvider {
    private val httpClient = mockk<OkHttpClient>()
    // private val queryBuilder = mockk<QueryBuilder>()
    private val queryBuilder = QueryBuilder()
    private val unit = VmRecordSetProvider(httpClient, queryBuilder)
    private val split = mockk<VmSplit>()
    private val session = mockk<ConnectorSession>()

    @Test
    fun testOneColumn() {
        val vmColumnHandles = listOf(VmColumnHandle("value"))
        val response = readFixture("export.response.ndjson")

        every { split.constraint } returns TupleDomain.all()
        every { httpClient.newCall(any()).execute() } returns response

        val recordSet = unit.getRecordSet(VmTransactionHandle(), session, split, vmColumnHandles)
        val cursor = recordSet.cursor()

        assertThat(cursor.advanceNextPosition()).isTrue()

        assertThat(cursor.getType(0)).isEqualTo(DoubleType.DOUBLE)

        // value
        assertThat(cursor.getDouble(0)).isEqualTo(21.0)
    }

    @Test
    fun testCompletedBytes() {
        val response = readFixture("export.response.ndjson")
        every { split.constraint } returns TupleDomain.all()
        every { httpClient.newCall(any()).execute() } returns response

        val recordSet = unit.getRecordSet(VmTransactionHandle(), session, split, listOf(VmColumnHandle("value")))
        val cursor = recordSet.cursor()

        for (expected in listOf(226L, 475L, 719L)) {
            for (i in 0..5) {
                assertThat(cursor.advanceNextPosition()).isTrue()
            }
            assertThat(cursor.completedBytes).isEqualTo(expected)
        }
        assertThat(cursor.advanceNextPosition()).isFalse()
    }

    @Test
    fun testAll() {
        val response = readFixture("export.response.ndjson")

        every { httpClient.newCall(any()).execute() } returns response
        every { split.constraint } returns (TupleDomain.all())

        val vmColumnHandles = listOf(
            VmColumnHandle("timestamp"),
            VmColumnHandle("value"),
            VmColumnHandle("name")
        )
        val recordSet = unit.getRecordSet(VmTransactionHandle(), session, split, vmColumnHandles)

        val cursor = recordSet.cursor()

        assertThat(cursor.advanceNextPosition()).isTrue()

        assertThat(cursor.getType(0)).isEqualTo(TimestampType.TIMESTAMP)
        assertThat(cursor.getType(1)).isEqualTo(DoubleType.DOUBLE)
        assertThat(cursor.getType(2)).isEqualTo(VarcharType.VARCHAR)

        // timestamp
        assertThatCode { cursor.getBoolean(0) }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatCode { cursor.getDouble(0) }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatCode { cursor.getSlice(0) }.isInstanceOf(IllegalArgumentException::class.java)
        assertThat(cursor.getLong(0)).isEqualTo(1571978321645L)

        // value
        // assertThrows<IllegalArgumentException> { cursor.getBoolean(1) }
        // assertThrows<IllegalArgumentException> { cursor.getLong(1) }
        // assertThrows<IllegalArgumentException> { cursor.getSlice(1) }
        // assertThat(cursor.getDouble(1)).isEqualTo(21.0)
        //
        // // name
        // assertThrows<IllegalArgumentException> { cursor.getBoolean(2) }
        // assertThrows<IllegalArgumentException> { cursor.getDouble(2) }
        // assertThrows<IllegalArgumentException> { cursor.getLong(2) }
        val name1 = """{"__name__":"vm_blocks","job":"vm","type":"indexdb","instance":"victoriametrics:8428"}"""
        assertThat(cursor.getSlice(2)).isEqualTo(Slices.utf8Slice(name1))

        // assertThrows<IllegalArgumentException> { cursor.getBoolean(3) }
        // assertThrows<IllegalArgumentException> { cursor.getLong(3) }
        // assertThrows<IllegalArgumentException> { cursor.getType(3) }

        cursor.advanceNextPosition()
        cursor.advanceNextPosition()
        assertThat(cursor.getLong(0)).isEqualTo(1571978365646L)
        assertThat(cursor.getDouble(1)).isEqualTo(23.0)

        cursor.advanceNextPosition()
        cursor.advanceNextPosition()
        cursor.advanceNextPosition()
        cursor.advanceNextPosition()
        assertThat(cursor.getLong(0)).isEqualTo(1571978321645L)
        assertThat(cursor.getDouble(1)).isEqualTo(61488.0)
        val name2 = """{"__name__":"vm_blocks","job":"vm","type":"storage/small","instance":"victoriametrics:8428"}"""
        assertThat(cursor.getSlice(2)).isEqualTo(Slices.utf8Slice(name2))

        cursor.close()
    }

    private fun readFixture(responseFileName: String): Response {
        val resource = javaClass.getResource(responseFileName)!!
        val content = resource.readText(StandardCharsets.UTF_8)

        return Response.Builder()
            .request(mockk())
            .code(200)
            .protocol(Protocol.HTTP_2)
            .message("Ok")
            .body(content.toResponseBody())
            .build()
    }
}
