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
package com.facebook.presto.operator.index;

import com.facebook.presto.Session;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSet;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
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

public class TestUnloadedIndexPageSet
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
        Function<PageSet, PageSet> probeKeyNormalizer = pageSource -> pageSource;

        Session session = testSessionBuilder().build();
        ConnectorIndex connectorIndex = new ConnectorIndex()
        {
            @Override
            public ConnectorPageSource lookup(PageSet pageSet)
            {
                return new SimplePageSource(pageSet.getPages());
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
                new IndexJoinLookupStats(), new PagesIndex.DefaultFactory(new OrderingCompiler(), joinCompiler, new FeaturesConfig()), joinCompiler);
        IndexSnapshot indexSnapshot = indexLoader.getIndexSnapshot();
        UnloadedIndexPageSet unloadedIndexKeyPageSource = new UnloadedIndexPageSet(session, indexSnapshot, ImmutableSet.of(1), recordSet.getColumnTypes(), ImmutableList.of(updateRequest), joinCompiler);
        Page resultIndexPage = unloadedIndexKeyPageSource.getPages().stream().findFirst().get();
        assertEquals(resultIndexPage.getPositionCount(), 3);
    }
}
