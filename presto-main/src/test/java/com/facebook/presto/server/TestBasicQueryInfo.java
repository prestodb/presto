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
package com.facebook.presto.server;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static org.testng.Assert.assertEquals;

public class TestBasicQueryInfo
{
    @Test
    public void testConstructor()
    {
        BasicQueryInfo basicInfo = new BasicQueryInfo(
                new QueryInfo(
                        new QueryId("0"),
                        TEST_SESSION.toSessionRepresentation(),
                        RUNNING,
                        new MemoryPoolId("reserved"),
                        false,
                        URI.create("1"),
                        ImmutableList.of("2", "3"),
                        "SELECT 4",
                        new QueryStats(
                                DateTime.parse("1991-09-06T05:00-05:30"),
                                DateTime.parse("1991-09-06T05:01-05:30"),
                                DateTime.parse("1991-09-06T05:02-05:30"),
                                DateTime.parse("1991-09-06T06:00-05:30"),
                                Duration.valueOf("8m"),
                                Duration.valueOf("7m"),
                                Duration.valueOf("34m"),
                                Duration.valueOf("9m"),
                                Duration.valueOf("10m"),
                                Duration.valueOf("11m"),
                                Duration.valueOf("12m"),
                                13,
                                14,
                                15,
                                16,
                                17,
                                18,
                                34,
                                19,
                                20.0,
                                DataSize.valueOf("21GB"),
                                DataSize.valueOf("22GB"),
                                DataSize.valueOf("23GB"),
                                DataSize.valueOf("24GB"),
                                DataSize.valueOf("25GB"),
                                true,
                                Duration.valueOf("23m"),
                                Duration.valueOf("24m"),
                                Duration.valueOf("25m"),
                                Duration.valueOf("26m"),
                                true,
                                ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY),
                                DataSize.valueOf("27GB"),
                                28,
                                DataSize.valueOf("29GB"),
                                30,
                                DataSize.valueOf("31GB"),
                                32,
                                DataSize.valueOf("32GB"),
                                ImmutableList.of(new StageGcStatistics(
                                        101,
                                        102,
                                        103,
                                        104,
                                        105,
                                        106,
                                        107)),
                                ImmutableList.of()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(),
                        ImmutableSet.of(),
                        ImmutableMap.of(),
                        ImmutableSet.of(),
                        Optional.empty(),
                        false,
                        "33",
                        Optional.empty(),
                        null,
                        StandardErrorCode.ABANDONED_QUERY.toErrorCode(),
                        ImmutableSet.of(),
                        Optional.empty(),
                        false,
                        Optional.empty()));

        assertEquals(basicInfo.getQueryId().getId(), "0");
        assertEquals(basicInfo.getState(), RUNNING);
        assertEquals(basicInfo.getMemoryPool().getId(), "reserved");
        assertEquals(basicInfo.isScheduled(), false);
        assertEquals(basicInfo.getQuery(), "SELECT 4");

        assertEquals(basicInfo.getQueryStats().getCreateTime(), DateTime.parse("1991-09-06T05:00-05:30"));
        assertEquals(basicInfo.getQueryStats().getEndTime(), DateTime.parse("1991-09-06T06:00-05:30"));
        assertEquals(basicInfo.getQueryStats().getElapsedTime(), Duration.valueOf("8m"));
        assertEquals(basicInfo.getQueryStats().getExecutionTime(), Duration.valueOf("1m"));

        assertEquals(basicInfo.getQueryStats().getTotalDrivers(), 16);
        assertEquals(basicInfo.getQueryStats().getQueuedDrivers(), 17);
        assertEquals(basicInfo.getQueryStats().getRunningDrivers(), 18);
        assertEquals(basicInfo.getQueryStats().getCompletedDrivers(), 19);

        assertEquals(basicInfo.getQueryStats().getCumulativeUserMemory(), 20.0);
        assertEquals(basicInfo.getQueryStats().getUserMemoryReservation(), DataSize.valueOf("21GB"));
        assertEquals(basicInfo.getQueryStats().getPeakUserMemoryReservation(), DataSize.valueOf("23GB"));
        assertEquals(basicInfo.getQueryStats().getTotalCpuTime(), Duration.valueOf("24m"));

        assertEquals(basicInfo.getQueryStats().isFullyBlocked(), true);
        assertEquals(basicInfo.getQueryStats().getBlockedReasons(), ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY));

        assertEquals(basicInfo.getQueryStats().getProgressPercentage(), OptionalDouble.of(100));

        assertEquals(basicInfo.getErrorCode(), StandardErrorCode.ABANDONED_QUERY.toErrorCode());
        assertEquals(basicInfo.getErrorType(), StandardErrorCode.ABANDONED_QUERY.toErrorCode().getType());
    }
}
