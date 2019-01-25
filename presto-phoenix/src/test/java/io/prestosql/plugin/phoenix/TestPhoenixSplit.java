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
package io.prestosql.plugin.phoenix;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestPhoenixSplit
{
    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    @Test
    public void testPhoenixSplitJsonRoundtrip()
            throws Exception
    {
        List<HostAddress> addresses = ImmutableList.of(HostAddress.fromString("host:9000"));
        List<Scan> scans = ImmutableList.of(new Scan().withStartRow(Bytes.toBytes("A")).withStopRow(Bytes.toBytes("Z")));
        PhoenixInputSplit phoenixInputSplit = new PhoenixInputSplit(scans);
        PhoenixSplit expected = new PhoenixSplit("phoenix", "testCatalogName", "testSchemaName", "testTableName", TupleDomain.all(), addresses, new WrappedPhoenixInputSplit(phoenixInputSplit));

        assertTrue(objectMapper.canSerialize(PhoenixSplit.class));

        String json = objectMapper.writeValueAsString(expected);
        PhoenixSplit actual = objectMapper.readValue(json, PhoenixSplit.class);
        assertEquals(actual, expected);
    }
}
