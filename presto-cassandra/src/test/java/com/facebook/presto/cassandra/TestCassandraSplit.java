/*
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
package com.facebook.presto.cassandra;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestCassandraSplit
{
    private final JsonCodec<CassandraSplit> codec = JsonCodec.jsonCodec(CassandraSplit.class);

    private final ImmutableList<HostAddress> addresses = ImmutableList.of(
            HostAddress.fromParts("127.0.0.1", 44),
            HostAddress.fromParts("127.0.0.1", 45));

    @Test
    public void testJsonRoundTrip()
    {
        CassandraSplit expected = new CassandraSplit("connectorId", "schema1", "table1", "partitionId", "condition", addresses);

        String json = codec.toJson(expected);
        CassandraSplit actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getSplitCondition(), expected.getSplitCondition());
        assertEquals(actual.getAddresses(), expected.getAddresses());
    }

    @Test
    public void testWhereClause()
    {
        CassandraSplit split;
        split = new CassandraSplit(
                "connectorId",
                "schema1",
                "table1",
                CassandraPartition.UNPARTITIONED_ID,
                "token(k) >= 0 AND token(k) <= 2",
                addresses);
        assertEquals(split.getWhereClause(), " WHERE token(k) >= 0 AND token(k) <= 2");

        split = new CassandraSplit(
                "connectorId",
                "schema1",
                "table1",
                "key = 123",
                null,
                addresses);
        assertEquals(split.getWhereClause(), " WHERE key = 123");
    }
}
