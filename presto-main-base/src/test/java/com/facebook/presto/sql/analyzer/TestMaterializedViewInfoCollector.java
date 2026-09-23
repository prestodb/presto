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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.eventlistener.MaterializedViewQueryInfo;
import com.facebook.presto.spi.eventlistener.MaterializedViewRewriteInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.FULLY_MATERIALIZED;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedViewState.PARTIALLY_MATERIALIZED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestMaterializedViewInfoCollector
{
    @Test
    public void testEmptyCollector()
    {
        MaterializedViewInfoCollector collector = new MaterializedViewInfoCollector();
        assertTrue(collector.getQueryInfos().isEmpty());
        assertTrue(collector.getRewriteInfos().isEmpty());
    }

    @Test
    public void testAddQueryInfoFullyMaterialized()
    {
        MaterializedViewInfoCollector collector = new MaterializedViewInfoCollector();
        MaterializedViewStatus status = new MaterializedViewStatus(FULLY_MATERIALIZED);
        collector.addQueryInfo("catalog.schema.mv1", status);

        List<MaterializedViewQueryInfo> infos = collector.getQueryInfos();
        assertEquals(infos.size(), 1);

        MaterializedViewQueryInfo info = infos.get(0);
        assertEquals(info.getMaterializedViewName(), "catalog.schema.mv1");
        assertEquals(info.getFreshnessState(), FULLY_MATERIALIZED);
        assertEquals(info.getStalePartitionCount(), 0);
        assertTrue(info.getBaseTableNames().isEmpty());
        assertTrue(info.getSampleStalePartitions().isEmpty());
    }

    @Test
    public void testAddQueryInfoPartiallyMaterialized()
    {
        MaterializedViewInfoCollector collector = new MaterializedViewInfoCollector();

        SchemaTableName baseTable = new SchemaTableName("schema1", "base_table");
        MaterializedDataPredicates predicates = new MaterializedDataPredicates(
                ImmutableList.of(TupleDomain.none(), TupleDomain.none()),
                ImmutableList.of("ds"));

        MaterializedViewStatus status = new MaterializedViewStatus(
                PARTIALLY_MATERIALIZED,
                ImmutableMap.of(baseTable, predicates),
                Optional.of(1000L));

        collector.addQueryInfo("catalog.schema.mv2", status);

        List<MaterializedViewQueryInfo> infos = collector.getQueryInfos();
        assertEquals(infos.size(), 1);

        MaterializedViewQueryInfo info = infos.get(0);
        assertEquals(info.getFreshnessState(), PARTIALLY_MATERIALIZED);
        assertEquals(info.getStalePartitionCount(), 2);
        assertEquals(info.getBaseTableNames(), ImmutableList.of("schema1.base_table"));
    }

    @Test
    public void testAddRewriteInfoSuccess()
    {
        MaterializedViewInfoCollector collector = new MaterializedViewInfoCollector();
        MaterializedViewRewriteInfo info = new MaterializedViewRewriteInfo(
                "schema.base_table",
                "schema.mv1",
                "SUCCESS",
                null);
        collector.addRewriteInfo(info);

        List<MaterializedViewRewriteInfo> infos = collector.getRewriteInfos();
        assertEquals(infos.size(), 1);
        assertEquals(infos.get(0).getBaseTableName(), "schema.base_table");
        assertEquals(infos.get(0).getMaterializedViewName(), "schema.mv1");
        assertEquals(infos.get(0).getStatus(), "SUCCESS");
        assertNull(infos.get(0).getFailureReason());
    }

    @Test
    public void testAddRewriteInfoFailure()
    {
        MaterializedViewInfoCollector collector = new MaterializedViewInfoCollector();
        MaterializedViewRewriteInfo info = new MaterializedViewRewriteInfo(
                "schema.base_table",
                "schema.mv1",
                "INCOMPATIBLE_SHAPE",
                "query shape does not match");
        collector.addRewriteInfo(info);

        List<MaterializedViewRewriteInfo> infos = collector.getRewriteInfos();
        assertEquals(infos.size(), 1);
        assertEquals(infos.get(0).getStatus(), "INCOMPATIBLE_SHAPE");
        assertEquals(infos.get(0).getFailureReason(), "query shape does not match");
    }

    @Test
    public void testMultipleMvsPerBaseTable()
    {
        MaterializedViewInfoCollector collector = new MaterializedViewInfoCollector();
        collector.addRewriteInfo(new MaterializedViewRewriteInfo(
                "schema.base_table",
                "schema.mv1",
                "SUCCESS",
                null));
        collector.addRewriteInfo(new MaterializedViewRewriteInfo(
                "schema.base_table",
                "schema.mv2",
                "INCOMPATIBLE_SHAPE",
                "missing columns"));

        List<MaterializedViewRewriteInfo> infos = collector.getRewriteInfos();
        assertEquals(infos.size(), 2);
        assertEquals(infos.get(0).getMaterializedViewName(), "schema.mv1");
        assertEquals(infos.get(1).getMaterializedViewName(), "schema.mv2");
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testReturnedQueryInfoListIsImmutable()
    {
        MaterializedViewInfoCollector collector = new MaterializedViewInfoCollector();
        collector.addQueryInfo("catalog.schema.mv1", new MaterializedViewStatus(FULLY_MATERIALIZED));
        collector.getQueryInfos().add(new MaterializedViewQueryInfo(
                "x", FULLY_MATERIALIZED, 0, ImmutableList.of(), ImmutableList.of()));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testReturnedRewriteInfoListIsImmutable()
    {
        MaterializedViewInfoCollector collector = new MaterializedViewInfoCollector();
        collector.addRewriteInfo(new MaterializedViewRewriteInfo(
                "schema.base", "schema.mv", "SUCCESS", null));
        collector.getRewriteInfos().add(new MaterializedViewRewriteInfo(
                "x", "y", "z", null));
    }
}
