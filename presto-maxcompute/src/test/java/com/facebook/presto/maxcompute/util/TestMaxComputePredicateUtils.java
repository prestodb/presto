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
package com.facebook.presto.maxcompute.util;

import com.aliyun.odps.PartitionSpec;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.maxcompute.MaxComputeColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.common.predicate.Domain.multipleValues;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.maxcompute.util.MaxComputePredicateUtils.convertToPredicate;
import static com.facebook.presto.maxcompute.util.MaxComputePredicateUtils.matchPartition;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMaxComputePredicateUtils
{
    private static final MaxComputeColumnHandle pt = new MaxComputeColumnHandle("odps", "pt", BIGINT, 0, false);
    private static final MaxComputeColumnHandle region = new MaxComputeColumnHandle("odps", "region", VARCHAR, 1, true);
    private static final List<MaxComputeColumnHandle> partitionColumns = Arrays.asList(pt, region);

    @Test
    public void testMatchPartition()
    {
        assertMatchPartition(
                partitionSpec("pt=2,region='hangzhou'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), true)));
        assertMatchPartition(
                partitionSpec("pt=3,region='hangzhou'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), true)));
        assertNotMatchPartition(
                partitionSpec("pt=1,region='hangzhou'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), true)));
        assertNotMatchPartition(
                partitionSpec("pt=0,region='hangzhou'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), true)));

        assertMatchPartition(
                partitionSpec("pt=1,region='hangzhou'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), true)));
        assertNotMatchPartition(
                partitionSpec("pt=0,region='hangzhou'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), true)));

        assertMatchPartition(
                partitionSpec("pt=2,region='20210309'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), true),
                    region, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("20210308"))), true)));
        assertMatchPartition(
                partitionSpec("pt=3,region='20210310'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), true),
                        region, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("20210308"))), true)));
        assertNotMatchPartition(
                partitionSpec("pt=2,region='20210308'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), true),
                        region, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("20210308"))), true)));
        assertNotMatchPartition(
                partitionSpec("pt=2,region='20210307'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), true),
                        region, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("20210308"))), true)));
        assertNotMatchPartition(
                partitionSpec("pt=1,region='20210309'"),
                tupledomain(pt, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), true),
                        region, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("20210308"))), true)));

        assertMatchPartition(
                partitionSpec("pt=1,region='hangzhou'"),
                tupledomain(pt, singleValue(BIGINT, 1L)));

        assertMatchPartition(
                partitionSpec("pt=1,region='hangzhou'"),
                tupledomain(region, singleValue(VARCHAR, utf8Slice("hangzhou"))));

        assertMatchPartition(
                partitionSpec("pt=1,region='hangzhou'"),
                tupledomain(pt, singleValue(BIGINT, 1L), region, singleValue(VARCHAR, utf8Slice("hangzhou"))));

        assertNotMatchPartition(
                partitionSpec("pt=2,region='beijing'"),
                tupledomain(pt, singleValue(BIGINT, 1L), region, singleValue(VARCHAR, utf8Slice("hangzhou"))));

        assertMatchPartition(
                partitionSpec("pt=2,region='beijing'"),
                tupledomain(pt, singleValue(BIGINT, 2L), region, singleValue(VARCHAR, utf8Slice("beijing"))));

        assertNotMatchPartition(
                partitionSpec("pt=2,region='beijing'"),
                tupledomain(pt, singleValue(BIGINT, 2L), region, singleValue(VARCHAR, utf8Slice("Beijing"))));

        assertMatchPartition(
                partitionSpec("pt=1"), tupledomain(pt, singleValue(BIGINT, 1L)));

        assertMatchPartition(
                partitionSpec("region='beijing'"), tupledomain(region, singleValue(VARCHAR, utf8Slice("beijing"))));

        assertMatchPartition(
                partitionSpec("region=1"), tupledomain(region, singleValue(VARCHAR, utf8Slice("1"))));

        assertMatchPartition(
                partitionSpec("pt=1"),
                tupledomain(pt, multipleValues(BIGINT, ImmutableList.of(1L, 2L))));
    }

    private void assertMatchPartition(PartitionSpec partitionSpec, TupleDomain<ColumnHandle> tupleDomain)
    {
        TupleDomain<ColumnHandle> partitionPredicates = TupleDomain.withColumnDomains(
                Maps.filterKeys(tupleDomain.getDomains().get(), Predicates.in(partitionColumns)));
        Predicate<Map<ColumnHandle, NullableValue>> predicate = convertToPredicate(partitionPredicates);
        assertTrue(matchPartition(partitionSpec, tupleDomain, partitionColumns, predicate));
    }

    private void assertNotMatchPartition(PartitionSpec partitionSpec, TupleDomain<ColumnHandle> tupleDomain)
    {
        TupleDomain<ColumnHandle> partitionPredicates = TupleDomain.withColumnDomains(
                Maps.filterKeys(tupleDomain.getDomains().get(), Predicates.in(partitionColumns)));
        Predicate<Map<ColumnHandle, NullableValue>> predicate = convertToPredicate(partitionPredicates);
        assertFalse(matchPartition(partitionSpec, tupleDomain, partitionColumns, predicate));
    }

    private static PartitionSpec partitionSpec(String str)
    {
        return new PartitionSpec(str);
    }

    @SuppressWarnings("unchecked")
    private TupleDomain<ColumnHandle> tupledomain(ColumnHandle ch, Domain d)
    {
        TupleDomain.ColumnDomain<ColumnHandle> cd = new ColumnDomain<>(ch, d);
        List<ColumnDomain<ColumnHandle>> list = Lists.newArrayList(cd);

        Optional<List<ColumnDomain<ColumnHandle>>> columnDomains = Optional.of(list);
        return TupleDomain.fromColumnDomains(columnDomains);
    }

    @SuppressWarnings("unchecked")
    private TupleDomain<ColumnHandle> tupledomain(ColumnHandle ch1, Domain d1, ColumnHandle ch2, Domain d2)
    {
        ColumnDomain<ColumnHandle> cd1 = new ColumnDomain<>(ch1, d1);
        ColumnDomain<ColumnHandle> cd2 = new ColumnDomain<>(ch2, d2);

        List<ColumnDomain<ColumnHandle>> list = Lists.newArrayList(cd1);
        list.add(cd2);

        Optional<List<ColumnDomain<ColumnHandle>>> columnDomains = Optional.of(list);
        return TupleDomain.fromColumnDomains(columnDomains);
    }
}
