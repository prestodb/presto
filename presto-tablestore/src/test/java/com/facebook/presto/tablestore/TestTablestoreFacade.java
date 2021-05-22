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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RangeIteratorParameter;
import com.alicloud.openservices.tablestore.model.Split;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter.LogicOperator;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.SearchIndexInfo;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.ExistsQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermsQuery;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;
import org.mockito.internal.util.collections.Sets;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder.createPrimaryKeyBuilder;
import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.INF_MAX;
import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.INF_MIN;
import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.fromLong;
import static com.alicloud.openservices.tablestore.model.PrimaryKeyValue.fromString;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.EXIST;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.GREATER_EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.GREATER_THAN;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.LESS_EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.LESS_THAN;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.NOT_EQUAL;
import static com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter.CompareOperator.NOT_EXIST;
import static com.facebook.presto.common.predicate.Range.equal;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.tablestore.TablestoreFacade.MAX_RANGE_TO_POINT_SIZE;
import static com.facebook.presto.tablestore.TablestoreFacade.buildQueryForSingleColumn;
import static com.facebook.presto.tablestore.TablestoreFacade.buildSearchRequestForData;
import static com.facebook.presto.tablestore.TablestoreFacade.buildSearchRequestForMatchedCount;
import static com.facebook.presto.tablestore.TablestoreFacade.buildSearchRequestForTotalCount;
import static com.facebook.presto.tablestore.TablestoreFacade.divideAndMergeAndShuffle;
import static com.facebook.presto.tablestore.TablestoreFacade.filterUsefulFields;
import static com.facebook.presto.tablestore.TablestoreFacade.findBestMatchedIndexAndBuildRequest;
import static com.facebook.presto.tablestore.TablestoreFacade.ifAllTupleDomainColumnsInIndex;
import static com.facebook.presto.tablestore.TablestoreFacade.isSingleValue;
import static com.facebook.presto.tablestore.TablestoreFacade.mergeAndTransform;
import static com.facebook.presto.tablestore.TablestoreFacade.printSearchQuery;
import static com.facebook.presto.tablestore.TablestoreFacade.printSearchRequest;
import static com.facebook.presto.tablestore.TablestoreFacade.toRangeMarker;
import static com.facebook.presto.tablestore.TablestoreFacade.toSingleValueMarker;
import static com.facebook.presto.tablestore.TablestoreFacade.transformFilterForColumnV2;
import static com.facebook.presto.tablestore.TablestoreFacade.transformRangeToSearchQuery;
import static com.facebook.presto.tablestore.TablestoreFacade.transformToFilterV2;
import static com.facebook.presto.tablestore.TablestoreFacade.transformToTableStoreQuery;
import static com.facebook.presto.tablestore.TablestoreSessionProperties.DEFAULT_FETCH_SIZE;
import static com.facebook.presto.tablestore.TablestoreSessionProperties.HINT_QUERY_VERSION;
import static com.google.common.collect.Sets.newHashSet;
import static io.airlift.slice.Slices.utf8Slice;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTablestoreFacade
        extends TablestoreConstants
{
    private static final String PK_NAME = "id";
    private static final PrimaryKey MIN_PK = pk(INF_MIN);
    private static final PrimaryKey MAX_PK = pk(INF_MAX);
    private static final String instance = "instance1";
    private static final SchemaTableName stn = new SchemaTableName("schema1", "table2");
    private static final ConnectorSession session = session();
    private static final String tableName = stn.getTableName();

    TablestoreColumnHandle pk0 = new TablestoreColumnHandle("p0", true, 0, INTEGER);
    TablestoreColumnHandle pk1 = new TablestoreColumnHandle("p1", true, 1, VARCHAR);
    TablestoreColumnHandle pk2 = new TablestoreColumnHandle("p2", true, 2, VARBINARY);
    TablestoreColumnHandle col3 = new TablestoreColumnHandle("c3", false, 0, INTEGER);
    TablestoreColumnHandle col4 = new TablestoreColumnHandle("c4", false, 0, INTEGER);
    List<TablestoreColumnHandle> cList = Lists.newArrayList(pk0, pk1, pk2, col3);

    private static PrimaryKey pk(PrimaryKeyValue pkv)
    {
        return createPrimaryKeyBuilder().addPrimaryKeyColumn(new PrimaryKeyColumn(TestTablestoreFacade.PK_NAME, pkv)).build();
    }

    private static PrimaryKey pk(PrimaryKeyColumn... pkv)
    {
        PrimaryKeyBuilder b = createPrimaryKeyBuilder();
        for (PrimaryKeyColumn p : pkv) {
            b.addPrimaryKeyColumn(p);
        }
        return b.build();
    }

    private static PrimaryKeyColumn pkLong(String name, long v)
    {
        return new PrimaryKeyColumn(name, fromLong(v));
    }

    private static PrimaryKeyColumn pkMin(String name)
    {
        return new PrimaryKeyColumn(name, INF_MIN);
    }

    private static PrimaryKeyColumn pkMax(String name)
    {
        return new PrimaryKeyColumn(name, INF_MAX);
    }

    @Test
    public void testToRangeMarker()
    {
        PrimaryKeyValue pk;

        pk = fromString("xxx");
        Marker m = toRangeMarker(VARCHAR, pk, false);
        Marker expected = Marker.exactly(VARCHAR, utf8Slice("xxx"));
        assertEquals(expected, m);

        pk = fromString("xxx");
        m = toRangeMarker(VARCHAR, pk, true);
        expected = Marker.below(VARCHAR, utf8Slice("xxx"));
        assertEquals(expected, m);

        pk = fromLong(123);
        m = toRangeMarker(BIGINT, pk, false);
        expected = Marker.exactly(BIGINT, 123L);
        assertEquals(expected, m);

        pk = fromLong(123);
        m = toRangeMarker(BIGINT, pk, true);
        expected = Marker.below(BIGINT, 123L);
        assertEquals(expected, m);

        expected = toRangeMarker(VARCHAR, PrimaryKeyValue.INF_MIN, false);
        assertEquals(Marker.lowerUnbounded(VARCHAR), expected);

        expected = toRangeMarker(VARCHAR, PrimaryKeyValue.INF_MAX, true);
        assertEquals(Marker.upperUnbounded(VARCHAR), expected);

        byte[] bin = new byte[] {1, 2, 3};
        pk = PrimaryKeyValue.fromBinary(bin);

        expected = toRangeMarker(VARBINARY, pk, false);
        assertEquals(Marker.exactly(VARBINARY, Slices.wrappedBuffer(bin)), expected);

        expected = toRangeMarker(VARBINARY, pk, true);
        assertEquals(Marker.below(VARBINARY, Slices.wrappedBuffer(bin)), expected);
    }

    @Test
    public void testIsSingleValue()
    {
        PrimaryKeyValue pk1 = fromString("xxx");
        PrimaryKeyValue pk2 = fromString("yyy");
        assertFalse(isSingleValue(pk1, pk2));

        pk1 = INF_MIN;
        assertFalse(isSingleValue(pk1, pk2));

        pk1 = fromString("xxx");
        pk2 = INF_MAX;
        assertFalse(isSingleValue(pk1, pk2));

        pk2 = fromString("xxx");
        assertTrue(isSingleValue(pk1, pk2));

        byte[] bin = new byte[] {1, 2, 3};
        pk1 = PrimaryKeyValue.fromBinary(bin);

        byte[] bin2 = new byte[] {1, 2, 3};
        pk2 = PrimaryKeyValue.fromBinary(bin2);

        assertTrue(isSingleValue(pk1, pk2));
    }

    @Test
    public void testToSingleValueMarker()
    {
        PrimaryKeyValue pk;

        pk = fromString("xxx");
        Marker m = toSingleValueMarker(VARCHAR, pk);
        Marker expected = Marker.exactly(VARCHAR, utf8Slice("xxx"));
        assertEquals(expected, m);

        pk = fromLong(123);
        m = toSingleValueMarker(BIGINT, pk);
        expected = Marker.exactly(BIGINT, 123L);
        assertEquals(expected, m);

        byte[] bin = new byte[] {1, 2, 3};
        pk = PrimaryKeyValue.fromBinary(bin);

        expected = toSingleValueMarker(VARBINARY, pk);
        assertEquals(Marker.exactly(VARBINARY, Slices.wrappedBuffer(bin)), expected);
    }

    @Test
    public void testMergeAndTransform()
    {
        ConnectorSession session = session();
        TablestoreTableHandle oth = new TablestoreTableHandle(stn, cList);

        TupleDomain<ColumnHandle> td = TupleDomain.all();
        Split split = new Split();
        split.setLocation("location1");
        PrimaryKey p1 = pk(pkLong("p0", 100), pkMin("p1"), pkMin("p2"));
        split.setLowerBound(p1);

        PrimaryKey p2 = pk(pkMax("p0"), pkMin("p1"), pkMin("p2"));
        split.setUpperBound(p2);

        session = session(HINT_QUERY_VERSION, 1);
        List<TablestoreSplit> tablestoreSplit = mergeAndTransform(session, instance, oth, split, td, 0);
        assertEquals(1, tablestoreSplit.size());

        RangeIteratorParameter r = transformToTableStoreQuery(session, tablestoreSplit.get(0), cList);
        assertEquals(tableName, r.getTableName());
        assertEquals(-1, r.getMaxCount());
        assertFalse(r.hasSetTimeRange());
        assertEquals(DEFAULT_FETCH_SIZE, r.getBufferSize());
        assertEquals(p1, r.getInclusiveStartPrimaryKey());
        assertEquals(p2, r.getExclusiveEndPrimaryKey());
        assertEquals(1, r.getMaxVersions());
        assertEquals(Sets.newSet("c3"), r.getColumnsToGet());

        // (-∞,0] ^ [100, +∞)
        Map<ColumnHandle, Domain> domains = new HashMap<>();
        domains.put(pk0, Domain.singleValue(INTEGER, 0L));
        td = TupleDomain.withColumnDomains(domains);
        tablestoreSplit = mergeAndTransform(session, instance, oth, split, td, 0);
        assertEquals(0, tablestoreSplit.size());

        // [200,200] ^ [100, +∞)
        domains = new HashMap<>();
        domains.put(pk0, Domain.singleValue(INTEGER, 200L));
        td = TupleDomain.withColumnDomains(domains);
        tablestoreSplit = mergeAndTransform(session, instance, oth, split, td, 0);
        assertEquals(1, tablestoreSplit.size());
        r = transformToTableStoreQuery(session, tablestoreSplit.get(0), cList);

        p1 = pk(pkLong("p0", 200), pkMin("p1"), pkMin("p2"));
        p2 = pk(pkLong("p0", 200), pkMax("p1"), pkMin("p2"));

        assertEquals(p1, r.getInclusiveStartPrimaryKey());
        assertEquals(p2, r.getExclusiveEndPrimaryKey());

        // (-∞, 300) ^ [100, +∞)
        domains = new HashMap<>();
        ValueSet vs = ValueSet.ofRanges(Range.lessThan(INTEGER, 300L));
        domains.put(pk0, Domain.create(vs, false));
        td = TupleDomain.withColumnDomains(domains);
        tablestoreSplit = mergeAndTransform(session, instance, oth, split, td, 100);
        assertEquals(1, tablestoreSplit.size());
        r = transformToTableStoreQuery(session, tablestoreSplit.get(0), cList);

        p1 = pk(pkLong("p0", 100), pkMin("p1"), pkMin("p2"));
        p2 = pk(pkLong("p0", 300), pkMin("p1"), pkMin("p2"));

        assertEquals(p1, r.getInclusiveStartPrimaryKey());
        assertEquals(p2, r.getExclusiveEndPrimaryKey());

        // New protocol support
        session = session(HINT_QUERY_VERSION, 2);
        domains = new HashMap<>();
        domains.put(pk0, Domain.singleValue(INTEGER, 200L));
        td = TupleDomain.withColumnDomains(domains);
        tablestoreSplit = mergeAndTransform(session, instance, oth, split, td, 40);
        r = transformToTableStoreQuery(session, tablestoreSplit.get(0), cList);
        assertEquals(cList.size(), r.getColumnsToGet().size());
        assertEquals(Sets.newSet("p0", "p1", "p2", "c3"), r.getColumnsToGet());
        // count(*)
        r = transformToTableStoreQuery(session, tablestoreSplit.get(0), new ArrayList<>());
        assertEquals(1, r.getColumnsToGet().size());
        assertEquals(Sets.newSet("p0"), r.getColumnsToGet());
    }

    @Test
    public void testMergeAndTransform1()
    {
        Split split = new Split();
        split.setLocation("location1");
        PrimaryKey p1 = pk(pkLong("p0", 2), pkMin("p1"), pkMin("p2"));
        PrimaryKey p2 = pk(pkMax("p0"), pkMin("p1"), pkMin("p2"));
        split.setLowerBound(p1);
        split.setUpperBound(p2);

        TablestoreTableHandle oth = new TablestoreTableHandle(stn, cList);

        Map<ColumnHandle, Domain> map = new HashMap<>();
        ValueSet v1 = ValueSet.of(INTEGER, 1L, 3L, 4L, 7L);
        map.put(pk0, Domain.create(v1, true));
        map.put(pk1, Domain.create(ValueSet.of(VARCHAR, utf8Slice("xxx"), utf8Slice("yyy")), true));
        map.put(col3, Domain.create(ValueSet.of(VARCHAR, utf8Slice("hhh"), utf8Slice("zzz")), true));
        TupleDomain<ColumnHandle> td = TupleDomain.withColumnDomains(map);

        List<TablestoreSplit> tablestoreSplit = mergeAndTransform(session, instance, oth, split, td, 0);
        assertEquals(3, tablestoreSplit.size());

        TablestoreSplit s0 = tablestoreSplit.get(0);
        Domain pp0 = s0.getMergedTupleDomain().getDomains().get().get(pk0);
        assertTrue(pp0.isSingleValue());
        assertEquals(3L, pp0.getSingleValue());

        Domain pp1 = s0.getMergedTupleDomain().getDomains().get().get(pk1);
        assertFalse(pp1.isNullAllowed()); // primary key, ignore null
        assertEquals(2, pp1.getValues().getRanges().getRangeCount()); //主键，忽略null

        Domain pp3 = s0.getMergedTupleDomain().getDomains().get().get(col3);
        assertTrue(pp3.isNullAllowed()); //non-primary key，do not ignore null

        TablestoreSplit s1 = tablestoreSplit.get(1);
        pp0 = s1.getMergedTupleDomain().getDomains().get().get(pk0);
        assertTrue(pp0.isSingleValue());

        TablestoreSplit s2 = tablestoreSplit.get(2);
        pp0 = s2.getMergedTupleDomain().getDomains().get().get(pk0);
        assertTrue(pp0.isSingleValue());
    }

    @Test
    public void testMergeAndTransformForTooManySmallRanges()
    {
        Split split = new Split();
        split.setLocation("location1");
        PrimaryKey p1 = pk(pkLong("p0", 1), pkMin("p1"), pkMin("p2"));
        PrimaryKey p2 = pk(pkMax("p0"), pkMin("p1"), pkMin("p2"));
        split.setLowerBound(p1);
        split.setUpperBound(p2);

        TablestoreTableHandle oth = new TablestoreTableHandle(stn, cList);
        Map<ColumnHandle, Domain> map = new HashMap<>();

        List<Object> partitionKeys = new ArrayList<>();
        for (int i = 0; i < MAX_RANGE_TO_POINT_SIZE + 1; i++) {
            partitionKeys.add(2 * i + 1L);
        }
        ValueSet v1 = ValueSet.copyOf(INTEGER, partitionKeys);
        map.put(pk0, Domain.create(v1, true));
        map.put(pk1, Domain.create(ValueSet.of(VARCHAR, utf8Slice("xxx"), utf8Slice("yyy")), true));
        map.put(col3, Domain.create(ValueSet.of(VARCHAR, utf8Slice("hhh"), utf8Slice("zzz")), true));
        TupleDomain<ColumnHandle> td = TupleDomain.withColumnDomains(map);

        List<TablestoreSplit> tablestoreSplit = mergeAndTransform(session, instance, oth, split, td, 0);
        assertEquals(1, tablestoreSplit.size());
    }

    private void doFilterAndEquals(String expected, ColumnValueFilter f)
    {
        StringBuilder sb = new StringBuilder();
        TablestoreFacade.printFilter(f, sb);
        assertEquals(expected, sb.toString());
    }

    @Test
    public void testPrintFilter()
    {
        doFilterAndEquals("", null);

        SingleColumnValueRegexFilter f = new SingleColumnValueRegexFilter("a", EXIST);
        doFilterAndEquals("a IS NOT NULL", f);

        f = new SingleColumnValueRegexFilter("a", NOT_EXIST);
        doFilterAndEquals("a IS NULL", f);

        f = new SingleColumnValueRegexFilter("a", LESS_EQUAL, ColumnValue.fromLong(3));
        doFilterAndEquals("a <= 3", f);

        f = new SingleColumnValueRegexFilter("a", LESS_THAN, ColumnValue.fromLong(3));
        doFilterAndEquals("a < 3", f);

        f = new SingleColumnValueRegexFilter("a", GREATER_EQUAL, ColumnValue.fromLong(3));
        doFilterAndEquals("a >= 3", f);

        f = new SingleColumnValueRegexFilter("a", GREATER_THAN, ColumnValue.fromLong(3));
        doFilterAndEquals("a > 3", f);

        f = new SingleColumnValueRegexFilter("a", NOT_EQUAL, ColumnValue.fromLong(3));
        doFilterAndEquals("a != 3", f);

        f = new SingleColumnValueRegexFilter("a", EQUAL, ColumnValue.fromLong(3));
        doFilterAndEquals("a = 3", f);

        SingleColumnValueRegexFilter f1 = new SingleColumnValueRegexFilter("a", EQUAL, ColumnValue.fromLong(1));
        SingleColumnValueRegexFilter f2 = new SingleColumnValueRegexFilter("a", LESS_THAN, ColumnValue.fromLong(3));
        SingleColumnValueRegexFilter f3 = new SingleColumnValueRegexFilter("a", GREATER_EQUAL, ColumnValue.fromLong(5));
        CompositeColumnValueFilter cf = new CompositeColumnValueFilter(LogicOperator.AND);
        cf.addFilter(f1).addFilter(f2).addFilter(f3);
        doFilterAndEquals("a = 1 AND a < 3 AND a >= 5", cf);

        cf = new CompositeColumnValueFilter(LogicOperator.OR);
        cf.addFilter(f1).addFilter(f2).addFilter(f3);
        doFilterAndEquals("a = 1 OR a < 3 OR a >= 5", cf);

        cf = new CompositeColumnValueFilter(LogicOperator.OR);
        cf.addFilter(f1).addFilter(f2);
        CompositeColumnValueFilter cf2 = new CompositeColumnValueFilter(LogicOperator.NOT);
        cf2.addFilter(cf);
        CompositeColumnValueFilter cf3 = new CompositeColumnValueFilter(LogicOperator.AND);
        cf3.addFilter(f3).addFilter(cf2);
        doFilterAndEquals("a >= 5 AND (NOT (a = 1 OR a < 3))", cf3);

        cf3 = new CompositeColumnValueFilter(LogicOperator.AND);
        cf3.addFilter(cf2).addFilter(f3);
        doFilterAndEquals("(NOT (a = 1 OR a < 3)) AND a >= 5", cf3);
    }

    @SuppressWarnings("SortedCollectionWithNonComparableKeys")
    @Test
    public void testTransformToFilterV2()
    {
        Map<ColumnHandle, Domain> d = new TreeMap<>();
        ColumnValueFilter f = transformToFilterV2(d);
        assertNull(f);

        d.put(pk0, Domain.all(INTEGER));
        f = transformToFilterV2(d);
        doFilterAndEquals("", f);

        d.put(pk0, Domain.singleValue(INTEGER, 1L));
        f = transformToFilterV2(d);
        doFilterAndEquals("p0 = 1", f);

        d.put(pk0, Domain.notNull(INTEGER));
        f = transformToFilterV2(d);
        doFilterAndEquals("", f); // primary key

        d.clear();
        d.put(col3, Domain.notNull(INTEGER));
        f = transformToFilterV2(d);
        doFilterAndEquals("c3 IS NOT NULL", f); // non-primary key

        d.clear();
        d.put(pk0, Domain.onlyNull(INTEGER));
        f = transformToFilterV2(d);
        doFilterAndEquals("", f);

        d.clear();
        d.put(col3, Domain.onlyNull(INTEGER));
        f = transformToFilterV2(d);
        doFilterAndEquals("c3 IS NULL", f);

        d.clear();
        d.put(pk0, Domain.multipleValues(INTEGER, Lists.newArrayList(1L, 3L, 5L)));
        f = transformToFilterV2(d);
        doFilterAndEquals("p0 = 1 OR p0 = 3 OR p0 = 5", f);

        d.clear();
        ValueSet vs = ValueSet.all(INTEGER);
        d.put(pk0, Domain.create(vs, true));
        f = transformToFilterV2(d);
        doFilterAndEquals("", f); // all

        d.clear();
        vs = ValueSet.all(INTEGER);
        d.put(pk1, Domain.create(vs, false));
        f = transformToFilterV2(d);
        doFilterAndEquals("", f); // primary key

        d.clear();
        vs = ValueSet.all(INTEGER);
        d.put(col3, Domain.create(vs, false));
        f = transformToFilterV2(d);
        doFilterAndEquals("c3 IS NOT NULL", f); // non-primary key

        d.clear();
        vs = ValueSet.copyOf(INTEGER, Lists.newArrayList(1L, 3L, 5L));
        d.put(pk0, Domain.create(vs, true)); // primary key，can not be null
        f = transformToFilterV2(d);
        doFilterAndEquals("p0 = 1 OR p0 = 3 OR p0 = 5", f);

        d.clear();
        vs = ValueSet.copyOf(INTEGER, Lists.newArrayList(1L, 3L, 5L));
        d.put(col3, Domain.create(vs, true)); // non-primary key ，can be null
        f = transformToFilterV2(d);
        doFilterAndEquals("c3 IS NULL OR c3 = 1 OR c3 = 3 OR c3 = 5", f);

        d.clear();
        vs = ValueSet.copyOf(INTEGER, Lists.newArrayList(1L, 3L, 5L));
        d.put(pk0, Domain.create(vs, false));
        f = transformToFilterV2(d);
        doFilterAndEquals("p0 = 1 OR p0 = 3 OR p0 = 5", f);

        d.clear();
        vs = ValueSet.ofRanges(
                Range.lessThan(INTEGER, 1L),
                equal(INTEGER, 3L),
                Range.range(INTEGER, 5L, false, 7L, true),
                Range.greaterThanOrEqual(INTEGER, 9L));
        d.put(pk0, Domain.create(vs, false));
        f = transformToFilterV2(d);
        doFilterAndEquals("p0 < 1 OR p0 = 3 OR (p0 > 5 AND p0 <= 7) OR p0 >= 9", f);

        d.clear();
        d.put(pk0, Domain.create(vs, true));
        d.put(pk1, Domain.singleValue(VARCHAR, utf8Slice("xx")));
        d.put(col3, Domain.notNull(INTEGER));
        f = transformToFilterV2(d);
        doFilterAndEquals("c3 IS NOT NULL AND (p0 < 1 OR p0 = 3 OR (p0 > 5 AND p0 <= 7) OR p0 >= 9) AND p1 = xx", f);

        d.clear();
        d.put(pk0, Domain.create(vs, true));
        d.put(pk1, Domain.singleValue(VARCHAR, utf8Slice("xx")));
        d.put(col3, Domain.create(ValueSet.of(INTEGER, 1L, 3L), true));
        f = transformToFilterV2(d);
        doFilterAndEquals("(c3 IS NULL OR c3 = 1 OR c3 = 3) AND (p0 < 1 OR p0 = 3 OR (p0 > 5 AND p0 <= 7) OR p0 >= 9) AND p1 = xx", f);
    }

    @Test
    public void testTransformRangeForColumn()
    {
        Range r = Range.all(INTEGER); // (-∞,+∞)
        ColumnValueFilter res = transformFilterForColumnV2(PK1_INT, r);
        assertNull(res);

        r = equal(INTEGER, 1L); // [1,1]
        res = transformFilterForColumnV2(PK1_INT, r);
        SingleColumnValueRegexFilter sf = (SingleColumnValueRegexFilter) res;
        assertNotNull(sf);
        assertEquals(PK1_INT, sf.getColumnName());
        assertEquals(1L, sf.getColumnValue().asLong());
        assertEquals(EQUAL, sf.getOperator());

        r = Range.lessThan(INTEGER, 10L); //(-∞,10)
        res = transformFilterForColumnV2(PK1_INT, r);
        sf = (SingleColumnValueRegexFilter) res;
        assertNotNull(sf);
        assertEquals(PK1_INT, sf.getColumnName());
        assertEquals(10L, sf.getColumnValue().asLong());
        assertEquals(LESS_THAN, sf.getOperator());

        r = Range.lessThanOrEqual(INTEGER, 10L); // (-∞,10]
        res = transformFilterForColumnV2(PK1_INT, r);
        sf = (SingleColumnValueRegexFilter) res;
        assertNotNull(sf);
        assertEquals(PK1_INT, sf.getColumnName());
        assertEquals(10L, sf.getColumnValue().asLong());
        assertEquals(LESS_EQUAL, sf.getOperator());

        r = Range.greaterThanOrEqual(INTEGER, 10L); // [10,+∞)
        res = transformFilterForColumnV2(PK1_INT, r);
        sf = (SingleColumnValueRegexFilter) res;
        assertNotNull(sf);
        assertEquals(PK1_INT, sf.getColumnName());
        assertEquals(10L, sf.getColumnValue().asLong());
        assertEquals(GREATER_EQUAL, sf.getOperator());

        r = Range.greaterThan(INTEGER, 10L); // (10,+∞)
        res = transformFilterForColumnV2(PK1_INT, r);
        sf = (SingleColumnValueRegexFilter) res;
        assertNotNull(sf);
        assertEquals(PK1_INT, sf.getColumnName());
        assertEquals(10L, sf.getColumnValue().asLong());
        assertEquals(GREATER_THAN, sf.getOperator());

        r = Range.range(INTEGER, 1L, true, 2L, false); //[1,2)
        res = transformFilterForColumnV2(PK1_INT, r);
        CompositeColumnValueFilter cf = (CompositeColumnValueFilter) res;
        assertNotNull(cf);
        assertEquals(LogicOperator.AND, cf.getOperationType());
        assertEquals(2, cf.getSubFilters().size());
        SingleColumnValueRegexFilter sf0 = (SingleColumnValueRegexFilter) cf.getSubFilters().get(0);
        SingleColumnValueRegexFilter sf1 = (SingleColumnValueRegexFilter) cf.getSubFilters().get(1);

        assertEquals(PK1_INT, sf0.getColumnName());
        assertEquals(1L, sf0.getColumnValue().asLong());
        assertEquals(GREATER_EQUAL, sf0.getOperator());

        assertEquals(PK1_INT, sf1.getColumnName());
        assertEquals(2L, sf1.getColumnValue().asLong());
        assertEquals(LESS_THAN, sf1.getOperator());
    }

    /**
     * Sample output：0:(min,0) 1:(0,1) 2:(1,2) 3:(2,3) 4:(3,4) 5:(4,5) 6:(5,6) 7:(6,7) 8:(7,8) 9:(8,max), the format is: <partition>:([min], [max])
     */
    private String print(List<Split> rList)
    {
        return rList.stream().map(s -> {
            PrimaryKey lower = s.getLowerBound();
            PrimaryKey upper = s.getUpperBound();
            String x = s.getLocation() + ":(" + (lower.equals(MIN_PK) ? "min" : lower.getPrimaryKeyColumn(0).getValue().asLong()) + ",";
            x += (upper.equals(MAX_PK) ? "max" : upper.getPrimaryKeyColumn(0).getValue().asLong()) + ")";
            return x;
        }).reduce((a, b) -> a + " " + b).orElseThrow(NullPointerException::new);
    }

    private List<Split> mockSplitList(int size, int numberPerLocation)
    {
        List<Split> list = new ArrayList<>();
        PrimaryKey lower = MIN_PK;
        for (int i = 0; i < size; i++) {
            Split s = new Split();
            s.setLocation("" + (i / numberPerLocation));
            PrimaryKey upper;
            if (i == size - 1) {
                upper = MAX_PK;
            }
            else {
                upper = pk(fromLong(i));
            }
            s.setLowerBound(lower);
            s.setUpperBound(upper);
            list.add(s);
            lower = upper;
        }
        return list;
    }

    @Test
    public void testDivideAndMergeAndShuffle()
    {
        List<Split> list = mockSplitList(10, 3);
        List<Split> rList = divideAndMergeAndShuffle(4, list);
        assertEquals("0:(min,3) 1:(3,5) 2:(5,7) 2:(7,max)", print(rList));

        list = mockSplitList(10, 3);
        rList = divideAndMergeAndShuffle(3, list);
        assertEquals("0:(min,3) 1:(3,7) 2:(7,max)", print(rList));
    }

    @Test
    public void testDivideAndMergeAndShuffle_roundRobin()
    {
        List<Split> list = mockSplitList(10, 3);

        List<Split> rList = divideAndMergeAndShuffle(8, list);
        //(min,0),(0,1),(1,2) | (2,3),(3,4),(4,5) | (5,6),(6,7),(7,8) | (8,max)  --> (min,1),(1,2) | (2,4),(4,5) | (5,6),(6,7),(7,8) | (8,max)
        assertEquals("0:(min,1) 1:(3,4) 2:(5,6) 3:(8,max) 0:(1,3) 1:(4,5) 2:(6,7) 2:(7,8)", print(rList));
    }

    @Test
    public void testDivideAndMergeAndShuffle_mergeAll()
    {
        List<Split> list = mockSplitList(10, 20);

        List<Split> rList = divideAndMergeAndShuffle(0, list);
        //(min,0),(0,1),(1,2) | (2,3),(3,4),(4,5) | (5,6),(6,7),(7,8) | (8,max)  --> (min,max)
        assertEquals("0:(min,max)", print(rList));
    }

    @Test
    public void testDivideAndMergeAndShuffle_mergeNothing_reorder()
    {
        List<Split> list = mockSplitList(10, 3);

        List<Split> rList = divideAndMergeAndShuffle(20, list);
        //(min,0),(0,1),(1,2) | (2,3),(3,4),(4,5) | (5,6),(6,7),(7,8) | (8,max)  --> (min,0),(0,1),(1,2) | (2,3),(3,4),(4,5) | (5,6),(6,7),(7,8) | (8,max)
        assertEquals("0:(min,0) 1:(2,3) 2:(5,6) 3:(8,max) 0:(0,1) 1:(3,4) 2:(6,7) 0:(1,2) 1:(4,5) 2:(7,8)", print(rList));
    }

    @Test
    public void testDivideAndMergeAndShuffle_performance()
    {
        for (int i = 0; i < 10; i++) {
            List<Split> list = mockSplitList(300000, 13456);
            long n1 = System.currentTimeMillis();
            List<Split> rList = divideAndMergeAndShuffle(10000, list);
            long n2 = System.currentTimeMillis();

            assertEquals(10000, rList.size());
            System.out.println("Testing performance-" + i + ": " + (n2 - n1) + "ms");
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void test_transformRangeToSearchQuery()
    {
        String col = "column1";
        long val4 = 4;
        long val5 = 5;
        ColumnValue cv4 = ColumnValue.fromLong(val4);
        ColumnValue cv5 = ColumnValue.fromLong(val5);

        Range range = Range.all(INTEGER);
        Optional<Query> x = transformRangeToSearchQuery(col, range);
        assertFalse(x.isPresent());

        range = equal(INTEGER, val4);
        x = transformRangeToSearchQuery(col, range);
        TermQuery tq = (TermQuery) x.get();
        assertEquals(col, tq.getFieldName());
        assertEquals(cv4, tq.getTerm());

        range = Range.greaterThan(INTEGER, val4);
        x = transformRangeToSearchQuery(col, range);
        RangeQuery rq = (RangeQuery) x.get();
        assertEquals(col, rq.getFieldName());
        assertEquals(cv4, rq.getFrom());
        assertFalse(rq.isIncludeLower());
        assertNull(rq.getTo());

        range = Range.greaterThanOrEqual(INTEGER, val4);
        x = transformRangeToSearchQuery(col, range);
        rq = (RangeQuery) x.get();
        assertEquals(col, rq.getFieldName());
        assertEquals(cv4, rq.getFrom());
        assertTrue(rq.isIncludeLower());
        assertNull(rq.getTo());

        range = Range.lessThan(INTEGER, val4);
        x = transformRangeToSearchQuery(col, range);
        rq = (RangeQuery) x.get();
        assertEquals(col, rq.getFieldName());
        assertNull(rq.getFrom());
        assertFalse(rq.isIncludeUpper());
        assertEquals(cv4, rq.getTo());

        range = Range.lessThanOrEqual(INTEGER, val4);
        x = transformRangeToSearchQuery(col, range);
        rq = (RangeQuery) x.get();
        assertEquals(col, rq.getFieldName());
        assertNull(rq.getFrom());
        assertTrue(rq.isIncludeUpper());
        assertEquals(cv4, rq.getTo());

        range = Range.range(INTEGER, val4, true, val5, false);
        x = transformRangeToSearchQuery(col, range);
        rq = (RangeQuery) x.get();
        assertEquals(col, rq.getFieldName());
        assertEquals(cv4, rq.getFrom());
        assertTrue(rq.isIncludeLower());
        assertEquals(cv5, rq.getTo());
        assertFalse(rq.isIncludeUpper());
    }

    @Test
    public void testBuildQueryForSingleColumn()
    {
        String columnName = "c3";
        long val0 = 0;
        long val4 = 4;
        long val5 = 5;
        long val10 = 10;
        ColumnHandle ch = col3;

        // where  1 = 1
        Domain d = Domain.all(INTEGER);
        TupleDomain.ColumnDomain<ColumnHandle> cd = new TupleDomain.ColumnDomain<>(ch, d);
        Query qq = buildQueryForSingleColumn(cd);
        assertEquals("NOT (c3 is not null)", printSearchQuery(qq));

        // where 1 = 0
        d = Domain.none(INTEGER);
        cd = new TupleDomain.ColumnDomain<>(ch, d);
        try {
            buildQueryForSingleColumn(cd);
            fail();
        }
        catch (Exception e) {
            assertEquals("Impossible state(isNone=true): c3", e.getMessage());
        }

        // where x is not null
        Domain d1 = Domain.notNull(INTEGER);
        cd = new TupleDomain.ColumnDomain<>(ch, d1);
        Query query = buildQueryForSingleColumn(cd);
        assertEquals(ExistsQuery.class, query.getClass());
        assertEquals(columnName, ((ExistsQuery) query).getFieldName());
        assertEquals("c3 is not null", printSearchQuery(query));

        // where x in (4,5)
        Domain d2 = Domain.multipleValues(INTEGER, ImmutableList.of(val4, val5));
        cd = new TupleDomain.ColumnDomain<>(ch, d2);
        query = buildQueryForSingleColumn(cd);
        assertEquals(TermsQuery.class, query.getClass());
        TermsQuery tq = (TermsQuery) query;
        assertEquals(columnName, tq.getFieldName());
        assertEquals(ImmutableList.of(
                ColumnValue.fromLong(val4), ColumnValue.fromLong(val5)
        ), tq.getTerms());
        assertEquals("c3 in (4, 5)", printSearchQuery(query));

        // where x is null
        Domain d3 = Domain.onlyNull(INTEGER);
        cd = new TupleDomain.ColumnDomain<>(ch, d3);
        query = buildQueryForSingleColumn(cd);
        BoolQuery bq = (BoolQuery) query;
        assertEquals(1, bq.getMustNotQueries().size());
        ExistsQuery eq = (ExistsQuery) bq.getMustNotQueries().get(0);
        assertEquals(columnName, eq.getFieldName());
        assertEquals("NOT (c3 is not null)", printSearchQuery(query));

        // where x = 4
        Domain d4 = Domain.singleValue(INTEGER, val4);
        cd = new TupleDomain.ColumnDomain<>(ch, d4);
        query = buildQueryForSingleColumn(cd);
        TermQuery tq1 = (TermQuery) query;
        assertEquals(columnName, tq1.getFieldName());
        assertEquals(ColumnValue.fromLong(val4), tq1.getTerm());
        System.out.println(printSearchQuery(query));
        assertEquals("c3 = 4", printSearchQuery(query));

        // (-∞,0) or [10,∞)
        Range r1 = Range.lessThan(INTEGER, val0);
        Range r2 = Range.greaterThanOrEqual(INTEGER, val10);
        ValueSet values = ValueSet.ofRanges(r1, r2);

        // (-∞,0) or [10,∞) or is null
        Domain d5 = Domain.create(values, true);
        cd = new TupleDomain.ColumnDomain<>(ch, d5);
        query = buildQueryForSingleColumn(cd);
        System.out.println(query);
        assertEquals("c3 < 0 OR c3 >= 10 OR NOT (c3 is not null)", printSearchQuery(query));

        // ((-∞,0) or [10,∞)) and  is not null  == // (-∞,0) or [10,∞)
        Domain d6 = Domain.create(values, false);
        cd = new TupleDomain.ColumnDomain<>(ch, d6);
        query = buildQueryForSingleColumn(cd);
        System.out.println(query);
        assertEquals("c3 < 0 OR c3 >= 10", printSearchQuery(query));

        // ((-∞,0) or [10,∞)) or is null or in (4,5)
        Domain d7 = Domain.union(Lists.newArrayList(d5, d2));
        cd = new TupleDomain.ColumnDomain<>(ch, d7);
        query = buildQueryForSingleColumn(cd);
        System.out.println(query);
        assertEquals("c3 < 0 OR c3 >= 10 OR NOT (c3 is not null) OR c3 in (4, 5)",
                printSearchQuery(query));

        // [0,10)
        Range r3 = Range.range(INTEGER, val0, true, val10, false);
        values = ValueSet.ofRanges(r3);
        Domain d8 = Domain.create(values, false);
        cd = new TupleDomain.ColumnDomain<>(ch, d8);
        query = buildQueryForSingleColumn(cd);
        System.out.println(query);
        assertEquals("c3 >= 0 AND c3 < 10", printSearchQuery(query));
    }

    @Test
    public void testBuildSearchRequestForData()
    {
        SearchIndexInfo sii = new SearchIndexInfo();
        sii.setIndexName("idx");
        sii.setTableName("tbl");
        TablestoreColumnHandle[] chs = new TablestoreColumnHandle[] {pk0, pk1, col3};
        long val0 = 0;
        long val10 = 10;

        SearchRequest x = buildSearchRequestForData(TupleDomain.all(), sii.getTableName(), sii.getIndexName(), chs);
        assertEquals("SELECT p0, p1, c3 FROM tbl:idx WHERE true", printSearchRequest(x));

        try {
            buildSearchRequestForData(TupleDomain.none(), sii.getTableName(), sii.getIndexName(), chs);
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Unsupported state to build: isNone() == true");
        }

        Map<ColumnHandle, Domain> ds = new HashMap<>();
        Range r3 = Range.range(INTEGER, val0, true, val10, false);
        ValueSet values = ValueSet.ofRanges(r3);
        Domain d8 = Domain.create(values, false);
        ds.put(pk0, d8);
        judgeSearchRequest("SELECT p0, p1, c3 FROM tbl:idx WHERE p0 >= 0 AND p0 < 10", ds, sii, chs);

        ValueSet v1 = ValueSet.of(pk1.getColumnType(), Slices.utf8Slice("aaa"));
        Domain d = Domain.create(v1, true);
        ds = new HashMap<>();
        ds.put(pk0, d8);
        ds.put(pk1, d);
        judgeSearchRequest("SELECT p0, p1, c3 FROM tbl:idx WHERE p0 >= 0 AND p0 < 10 AND p1 = aaa OR NOT (p1 is not null)",
                ds, sii, chs);

        v1 = ValueSet.of(pk1.getColumnType(), Slices.utf8Slice("aaa"));
        d = Domain.create(v1, false);
        Domain d2 = Domain.create(ValueSet.of(col3.getColumnType(), 1L, 2L, 3L), false);

        ds = new HashMap<>();
        ds.put(pk0, d8);
        ds.put(pk1, d);
        ds.put(col3, d2);
        judgeSearchRequest("SELECT p0, p1, c3 FROM tbl:idx WHERE c3 in (1, 2, 3) AND p0 >= 0 AND p0 < 10 AND p1 = aaa",
                ds, sii, chs);

        chs = new TablestoreColumnHandle[0];
        judgeSearchRequest("SELECT count(*) FROM tbl:idx WHERE c3 in (1, 2, 3) AND p0 >= 0 AND p0 < 10 AND p1 = aaa", ds, sii, chs);
    }

    private void judgeSearchRequest(String expectedString, Map<ColumnHandle, Domain> ds, SearchIndexInfo sii, TablestoreColumnHandle[] chs)
    {
        TupleDomain<ColumnHandle> mtd = TupleDomain.withColumnDomains(ds);
        SearchRequest sr = buildSearchRequestForData(mtd, sii.getTableName(), sii.getIndexName(), chs);
        String x = printSearchRequest(sr);
        assertEquals(expectedString, x);
    }

    @Test
    public void testBuildSearchRequestForMatchedCount()
    {
        SearchIndexInfo sii = new SearchIndexInfo();
        sii.setIndexName("idx");
        sii.setTableName("tbl");
        long val0 = 0;
        long val10 = 10;

        SearchRequest x = buildSearchRequestForMatchedCount(TupleDomain.all(), sii.getTableName(), sii.getIndexName());
        assertEquals("SELECT count(*) FROM tbl:idx WHERE true LIMIT 0", printSearchRequest(x));

        try {
            buildSearchRequestForMatchedCount(TupleDomain.none(), sii.getTableName(), sii.getIndexName());
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Unsupported state to build: isNone() == true");
        }

        Map<ColumnHandle, Domain> ds = new HashMap<>();
        Range r3 = Range.range(INTEGER, val0, true, val10, false);
        ValueSet values = ValueSet.ofRanges(r3);
        Domain d8 = Domain.create(values, false);
        ds.put(pk0, d8);
        judgeSearchRequest4MatchedCount("SELECT count(*) FROM tbl:idx WHERE p0 >= 0 AND p0 < 10 LIMIT 0", ds, sii);

        ValueSet v1 = ValueSet.of(pk1.getColumnType(), Slices.utf8Slice("aaa"));
        Domain d = Domain.create(v1, true);
        ds = new HashMap<>();
        ds.put(pk0, d8);
        ds.put(pk1, d);
        judgeSearchRequest4MatchedCount("SELECT count(*) FROM tbl:idx WHERE p0 >= 0 AND p0 < 10 AND p1 = aaa OR NOT (p1 is not null) LIMIT 0",
                ds, sii);

        v1 = ValueSet.of(pk1.getColumnType(), Slices.utf8Slice("aaa"));
        d = Domain.create(v1, false);
        Domain d2 = Domain.create(ValueSet.of(col3.getColumnType(), 1L, 2L, 3L), false);

        ds = new HashMap<>();
        ds.put(pk0, d8);
        ds.put(pk1, d);
        ds.put(col3, d2);
        judgeSearchRequest4MatchedCount("SELECT count(*) FROM tbl:idx WHERE c3 in (1, 2, 3) AND p0 >= 0 AND p0 < 10 AND p1 = aaa LIMIT 0",
                ds, sii);
    }

    private void judgeSearchRequest4MatchedCount(String expectedString, Map<ColumnHandle, Domain> ds, SearchIndexInfo sii)
    {
        TupleDomain<ColumnHandle> mtd = TupleDomain.withColumnDomains(ds);
        SearchRequest sr = buildSearchRequestForMatchedCount(mtd, sii.getTableName(), sii.getIndexName());
        String x = printSearchRequest(sr);
        assertEquals(expectedString, x);
        assertTrue(sr.getSearchQuery().isGetTotalCount());
    }

    @Test
    public void testBuildSearchRequestForTotalCount()
    {
        SearchIndexInfo sii = new SearchIndexInfo();
        sii.setIndexName("idx");
        sii.setTableName("tbl");

        String expectedString = "SELECT count(*) FROM tbl:idx WHERE true LIMIT 0";

        SearchRequest sr = buildSearchRequestForTotalCount(sii);
        String x = printSearchRequest(sr);
        assertEquals(expectedString, x);
        assertTrue(sr.getSearchQuery().isGetTotalCount());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFindAllTupleDomainColumnsInIndex()
    {
        List<TupleDomain.ColumnDomain<ColumnHandle>> columnDomains = new ArrayList<>();
        Set<String> fieldSchemas = new HashSet<>();
        assertTrue(ifAllTupleDomainColumnsInIndex(columnDomains, fieldSchemas));

        Domain d = Domain.singleValue(INTEGER, 1L);
        TupleDomain.ColumnDomain<ColumnHandle> cd1 = new TupleDomain.ColumnDomain<>(pk1, d);
        TupleDomain.ColumnDomain<ColumnHandle> cd3 = new TupleDomain.ColumnDomain<>(col3, d);
        FieldSchema fs1 = new FieldSchema(pk1.getColumnName(), FieldType.LONG);
        FieldSchema fs3 = new FieldSchema(col3.getColumnName(), FieldType.LONG);
        FieldSchema fs11 = new FieldSchema(pk1.getColumnName(), FieldType.GEO_POINT);

        columnDomains = ImmutableList.of(cd1, cd3);
        fieldSchemas = ImmutableSet.of(fs1.getFieldName());
        assertFalse(ifAllTupleDomainColumnsInIndex(columnDomains, fieldSchemas));

        columnDomains = Lists.newArrayList(cd1);
        fieldSchemas = newHashSet(fs1.getFieldName(), fs3.getFieldName());
        assertTrue(ifAllTupleDomainColumnsInIndex(columnDomains, fieldSchemas));

        columnDomains = Lists.newArrayList(cd1);
        fieldSchemas = newHashSet(fs1.getFieldName());
        assertTrue(ifAllTupleDomainColumnsInIndex(columnDomains, fieldSchemas));

        columnDomains = Lists.newArrayList(cd1);
        fieldSchemas = newHashSet(fs11.getFieldName());
        assertTrue(ifAllTupleDomainColumnsInIndex(columnDomains, fieldSchemas));
    }

    @Test
    public void testFilterUsefulFields()
    {
        FieldSchema fs1 = new FieldSchema("f1", FieldType.LONG);
        FieldSchema fs2 = new FieldSchema("f2", FieldType.GEO_POINT);
        FieldSchema fs3 = new FieldSchema("f3", FieldType.KEYWORD);
        FieldSchema fs4 = new FieldSchema("f4", FieldType.DOUBLE);
        FieldSchema fs5 = new FieldSchema("f5", FieldType.BOOLEAN);
        FieldSchema fs6 = new FieldSchema("f6", FieldType.NESTED);
        FieldSchema fs7 = new FieldSchema("f7", FieldType.TEXT);
        FieldSchema fs8 = new FieldSchema("f8", FieldType.KEYWORD);

        List<FieldSchema> list = Collections.emptyList();
        Set<String> set = filterUsefulFields(list);
        assertEquals(0, set.size());

        list = Lists.newArrayList(fs1, fs2, fs3, fs4, fs5, fs6, fs7, fs8);
        set = filterUsefulFields(list);
        assertEquals(newHashSet("f1", "f3", "f4", "f5", "f8"), set);
    }

    @Test
    public void testFindBestMatchedIndexAndBuildRequest()
    {
        TablestoreTableHandle th = mock(TablestoreTableHandle.class);
        when(th.getTableName()).thenReturn("tbl");

        Map<ColumnHandle, Domain> map = new HashMap<>();
        map.put(pk0, Domain.create(ValueSet.of(INTEGER, 1L), false));
        map.put(col3, Domain.create(ValueSet.of(VARCHAR, utf8Slice("hhh")), false));
        TupleDomain<ColumnHandle> td = TupleDomain.withColumnDomains(map);

        Map<String, Set<String>> ic = new TreeMap<>();
        ic.put("index1", newHashSet(pk0.getColumnName()));
        ic.put("index2", newHashSet(pk0.getColumnName(), col3.getColumnName()));

        //select count(*)
        TablestoreSplit split = new TablestoreSplit(th, td, "", ic);
        WrappedSearchRequest p = findBestMatchedIndexAndBuildRequest("queryId", split, new TablestoreColumnHandle[0]);
        assertTrue(p.isOnlyFetchRowCount());
        assertEquals("SELECT count(*) FROM tbl:index1 WHERE c3 = hhh AND p0 = 1 LIMIT 0", printSearchRequest(p.getSearchRequest()));

        // No covering index(col4)，use any index
        TablestoreColumnHandle[] chs = new TablestoreColumnHandle[] {pk0, col4};
        p = findBestMatchedIndexAndBuildRequest("queryId", split, chs);
        assertFalse(p.isOnlyFetchRowCount());
        assertEquals("SELECT p0, c4 FROM tbl:index1 WHERE c3 = hhh AND p0 = 1", printSearchRequest(p.getSearchRequest()));

        // Covering index
        chs = new TablestoreColumnHandle[] {pk0, col3};
        p = findBestMatchedIndexAndBuildRequest("queryId", split, chs);
        assertFalse(p.isOnlyFetchRowCount());
        assertEquals("SELECT p0, c3 FROM tbl:index2 WHERE c3 = hhh AND p0 = 1", printSearchRequest(p.getSearchRequest()));

        // Although pk1 is not in the index, but actually ALL index contains the primary under the hood
        chs = new TablestoreColumnHandle[] {pk0, pk1, col3};
        p = findBestMatchedIndexAndBuildRequest("queryId", split, chs);
        assertFalse(p.isOnlyFetchRowCount());
        assertEquals("SELECT p0, p1, c3 FROM tbl:index2 WHERE c3 = hhh AND p0 = 1", printSearchRequest(p.getSearchRequest()));
    }
}
