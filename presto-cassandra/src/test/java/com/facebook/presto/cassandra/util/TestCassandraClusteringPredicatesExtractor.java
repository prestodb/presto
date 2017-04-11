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
package com.facebook.presto.cassandra.util;

import com.facebook.presto.cassandra.CassandraClusteringPredicatesExtractor;
import com.facebook.presto.cassandra.CassandraColumnHandle;
import com.facebook.presto.cassandra.CassandraTable;
import com.facebook.presto.cassandra.CassandraTableHandle;
import com.facebook.presto.cassandra.CassandraType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestCassandraClusteringPredicatesExtractor
{
    private static CassandraColumnHandle col1;
    private static CassandraColumnHandle col2;
    private static CassandraColumnHandle col3;
    private static CassandraColumnHandle col4;
    private static CassandraTable cassandraTable;

    @BeforeTest
    void setUp()
            throws Exception
    {
        col1 = new CassandraColumnHandle("cassandra", "partitionKey1", 1, CassandraType.BIGINT, null, true, false, false, false);
        col2 = new CassandraColumnHandle("cassandra", "clusteringKey1", 2, CassandraType.BIGINT, null, false, true, false, false);
        col3 = new CassandraColumnHandle("cassandra", "clusteringKey2", 3, CassandraType.BIGINT, null, false, true, false, false);
        col4 = new CassandraColumnHandle("cassandra", "clusteringKe3", 4, CassandraType.BIGINT, null, false, true, false, false);

        cassandraTable = new CassandraTable(
                new CassandraTableHandle("cassandra", "test", "records"), ImmutableList.of(col1, col2, col3, col4));
    }

    @Test
    public void testBuildClusteringPredicate()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        col1, Domain.singleValue(BIGINT, 23L),
                        col2, Domain.singleValue(BIGINT, 34L),
                        col4, Domain.singleValue(BIGINT, 26L)));
        CassandraClusteringPredicatesExtractor predicatesExtractor = new CassandraClusteringPredicatesExtractor(cassandraTable.getClusteringKeyColumns(), tupleDomain);
        List<String> predicate = predicatesExtractor.getClusteringKeyPredicates();
        assertEquals(predicate.get(0), new StringBuilder("\"clusteringKey1\" = 34").toString());
    }

    @Test
    public void testGetUnenforcedPredicates()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        col2, Domain.singleValue(BIGINT, 34L),
                        col4, Domain.singleValue(BIGINT, 26L)));
        CassandraClusteringPredicatesExtractor predicatesExtractor = new CassandraClusteringPredicatesExtractor(cassandraTable.getClusteringKeyColumns(), tupleDomain);
        TupleDomain<ColumnHandle> unenforcedPredicates = TupleDomain.withColumnDomains(ImmutableMap.of(col4, Domain.singleValue(BIGINT, 26L)));
        assertEquals(predicatesExtractor.getUnenforcedConstraints(), unenforcedPredicates);
    }
}
