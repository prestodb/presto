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
package com.facebook.presto.spi.eventlistener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.PARTIALLY_MATERIALIZED;
import static org.testng.Assert.assertEquals;

public class TestMaterializedViewEventClasses
{
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module());

    @Test
    public void testQueryInfoRoundtrip()
            throws Exception
    {
        MaterializedViewQueryInfo original = new MaterializedViewQueryInfo(
                "catalog.schema.mv1",
                PARTIALLY_MATERIALIZED,
                3,
                Arrays.asList("schema.base1", "schema.base2"),
                Arrays.asList("ds=2024-01-01", "ds=2024-01-02"));

        String json = MAPPER.writeValueAsString(original);
        MaterializedViewQueryInfo deserialized = MAPPER.readValue(json, MaterializedViewQueryInfo.class);

        assertEquals(deserialized.getMaterializedViewName(), original.getMaterializedViewName());
        assertEquals(deserialized.getFreshnessState(), original.getFreshnessState());
        assertEquals(deserialized.getStalePartitionCount(), original.getStalePartitionCount());
        assertEquals(deserialized.getBaseTableNames(), original.getBaseTableNames());
        assertEquals(deserialized.getSampleStalePartitions(), original.getSampleStalePartitions());
    }

    @Test
    public void testRewriteInfoRoundtrip()
            throws Exception
    {
        MaterializedViewRewriteInfo original = new MaterializedViewRewriteInfo(
                "schema.base_table",
                "schema.mv1",
                "SUCCESS",
                null);

        String json = MAPPER.writeValueAsString(original);
        MaterializedViewRewriteInfo deserialized = MAPPER.readValue(json, MaterializedViewRewriteInfo.class);

        assertEquals(deserialized.getBaseTableName(), original.getBaseTableName());
        assertEquals(deserialized.getMaterializedViewName(), original.getMaterializedViewName());
        assertEquals(deserialized.getStatus(), original.getStatus());
        assertEquals(deserialized.getFailureReason(), original.getFailureReason());
    }

    @Test
    public void testRewriteInfoRoundtripWithFailureReason()
            throws Exception
    {
        MaterializedViewRewriteInfo original = new MaterializedViewRewriteInfo(
                "schema.base_table",
                "schema.mv2",
                "INCOMPATIBLE_SHAPE",
                "query shape does not match MV definition");

        String json = MAPPER.writeValueAsString(original);
        MaterializedViewRewriteInfo deserialized = MAPPER.readValue(json, MaterializedViewRewriteInfo.class);

        assertEquals(deserialized.getBaseTableName(), original.getBaseTableName());
        assertEquals(deserialized.getMaterializedViewName(), original.getMaterializedViewName());
        assertEquals(deserialized.getStatus(), original.getStatus());
        assertEquals(deserialized.getFailureReason(), "query shape does not match MV definition");
    }
}
