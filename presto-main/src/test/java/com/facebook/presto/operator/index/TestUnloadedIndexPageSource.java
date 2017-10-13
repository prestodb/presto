package com.facebook.presto.operator.index;

import com.facebook.presto.Session;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.IndexPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.StructuralTestUtil.arrayBlockOf;
import static org.testng.Assert.assertEquals;

public class TestUnloadedIndexPageSource
{
    @Test
    void Test()
    {
        ArrayType arrayOfBigintType = new ArrayType(BIGINT);
        RecordSet recordSet = new InMemoryRecordSet(
                ImmutableList.of(BIGINT, BIGINT, TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, arrayOfBigintType, arrayOfBigintType),
                ImmutableList.of(
                        ImmutableList.of(
                                100L,
                                100L,
                                // test same time in different time zone to make sure equal check was done properly
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(123)),
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(234)),
                                // test structural type
                                arrayBlockOf(BIGINT, 12, 34, 56),
                                arrayBlockOf(BIGINT, 12, 34, 56)),
                        ImmutableList.of(
                                101L,
                                100L,
                                // test same time in different time zone to make sure equal check was done properly
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(123)),
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(234)),
                                // test structural type
                                arrayBlockOf(BIGINT, 12, 34, 56),
                                arrayBlockOf(BIGINT, 12, 34, 56)),
                        ImmutableList.of(
                                102L,
                                102L,
                                // test same time in different time zone to make sure equal check was done properly
                                packDateTimeWithZone(103, getTimeZoneKeyForOffset(123)),
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(234)),
                                // test structural type
                                arrayBlockOf(BIGINT, 12, 34, 56),
                                arrayBlockOf(BIGINT, 12, 34, 56)),
                        ImmutableList.of(
                                103L,
                                103L,
                                // test same time in different time zone to make sure equal check was done properly
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(123)),
                                packDateTimeWithZone(100, getTimeZoneKeyForOffset(234)),
                                // test structural type
                                arrayBlockOf(BIGINT, 12, 56),
                                arrayBlockOf(BIGINT, 12, 34, 56))));
        Page page = new RecordPageSource(recordSet).getNextPage();
        UpdateRequest updateRequest = new UpdateRequest(page.getBlocks());
        Function<IndexPageSource, IndexPageSource> probeKeyNormalizer = pageSource -> pageSource;

        Session session = testSessionBuilder().build();
        ConnectorIndex connectorIndex = new ConnectorIndex()
        {
            @Override
            public ConnectorPageSource lookup(IndexPageSource pageSource)
            {
                return pageSource;
            }
        };
        OperatorFactory operatorFactory = new IndexSourceOperator.IndexSourceOperatorFactory(0, new PlanNodeId("0"), connectorIndex, recordSet.getColumnTypes(), probeKeyNormalizer);
        IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider = new IndexBuildDriverFactoryProvider(
                0,
                0,
                new PlanNodeId("0"),
                true,
                ImmutableList.of(operatorFactory),
                Optional.empty());
        JoinCompiler joinCompiler = new JoinCompiler();
        IndexLoader indexLoader = new IndexLoader(ImmutableSet.of(0), ImmutableList.of(0, 1, 2, 4), OptionalInt.of(0), recordSet.getColumnTypes(), indexBuildDriverFactoryProvider, 4, DataSize.valueOf("1MB"),
                new IndexJoinLookupStats(), new PagesIndex.DefaultFactory(new OrderingCompiler(), joinCompiler), joinCompiler);
        IndexSnapshot indexSnapshot = indexLoader.getIndexSnapshot();
        UnloadedIndexKeyRecordSet unloadedIndexKeyRecordSet = new UnloadedIndexKeyRecordSet(session, indexSnapshot, ImmutableSet.of(1), recordSet.getColumnTypes(), ImmutableList.of(updateRequest), joinCompiler);
        UnloadedIndexPageSource unloadedIndexKeyPageSource = new UnloadedIndexPageSource(session, indexSnapshot, ImmutableSet.of(1), recordSet.getColumnTypes(), ImmutableList.of(updateRequest), joinCompiler);
        Page resultIndexPage = unloadedIndexKeyPageSource.getNextPage();
        MaterializedResult result =  MaterializedResult.resultBuilder(session, recordSet.getColumnTypes()).page(resultIndexPage).build();
        assertEquals(resultIndexPage.getPositionCount(), 3);
    }
}
