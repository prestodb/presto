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

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_RUN;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.testng.Assert.assertEquals;

public class TestResourceGroupStateInfo
{
    @Test
    public void testJsonRoundTrip()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(ImmutableList.of("test", "user"));
        ResourceGroupStateInfo expected = new ResourceGroupStateInfo(
                resourceGroupId,
                CAN_RUN,
                new DataSize(10, GIGABYTE),
                new DataSize(100, BYTE),
                10,
                100,
                new Duration(1, HOURS),
                new Duration(10, HOURS),
                ImmutableList.of(new QueryStateInfo(
                        new QueryId("test_query"),
                        RUNNING,
                        Optional.of(resourceGroupId),
                        "SELECT * FROM t",
                        DateTime.parse("2017-06-12T21:39:48.658Z"),
                        "test_user",
                        Optional.of("catalog"),
                        Optional.of("schema"),
                        Optional.empty(),
                        Optional.of(new QueryProgressStats(
                                DateTime.parse("2017-06-12T21:39:50.966Z"),
                                150060,
                                243,
                                1541,
                                566038,
                                1680000,
                                24,
                                124539,
                                8283750,
                                false,
                                OptionalDouble.empty())))),
                0);
        JsonCodec<ResourceGroupStateInfo> codec = JsonCodec.jsonCodec(ResourceGroupStateInfo.class);
        ResourceGroupStateInfo actual = codec.fromJson(codec.toJson(expected));

        assertEquals(actual.getId(), resourceGroupId);
        assertEquals(actual.getState(), CAN_RUN);
        assertEquals(actual.getSoftMemoryLimit(), new DataSize(10, GIGABYTE));
        assertEquals(actual.getMemoryUsage(), new DataSize(100, BYTE));
        assertEquals(actual.getMaxRunningQueries(), 10);
        assertEquals(actual.getRunningTimeLimit(), new Duration(1, HOURS));
        assertEquals(actual.getMaxQueuedQueries(), 100);
        assertEquals(actual.getQueuedTimeLimit(), new Duration(10, HOURS));
        assertEquals(actual.getNumQueuedQueries(), 0);
        assertEquals(actual.getRunningQueries().size(), 1);
        QueryStateInfo queryStateInfo = actual.getRunningQueries().get(0);
        assertEquals(queryStateInfo.getQueryId(), new QueryId("test_query"));
        assertEquals(queryStateInfo.getQueryState(), RUNNING);
        assertEquals(queryStateInfo.getResourceGroupId(), Optional.of(resourceGroupId));
        assertEquals(queryStateInfo.getQuery(), "SELECT * FROM t");
        assertEquals(queryStateInfo.getCreateTime(), DateTime.parse("2017-06-12T21:39:48.658Z"));
        assertEquals(queryStateInfo.getUser(), "test_user");
        assertEquals(queryStateInfo.getCatalog(), Optional.of("catalog"));
        assertEquals(queryStateInfo.getSchema(), Optional.of("schema"));
        assertEquals(queryStateInfo.getResourceGroupChain(), Optional.empty());
        QueryProgressStats progressStats = queryStateInfo.getProgress().get();
        assertEquals(progressStats.getExecutionStartTime(), DateTime.parse("2017-06-12T21:39:50.966Z"));
        assertEquals(progressStats.getElapsedTimeMillis(), 150060);
        assertEquals(progressStats.getQueuedTimeMillis(), 243);
        assertEquals(progressStats.getCpuTimeMillis(), 1541);
        assertEquals(progressStats.getScheduledTimeMillis(), 566038);
        assertEquals(progressStats.getBlockedTimeMillis(), 1680000);
        assertEquals(progressStats.getPeakMemoryBytes(), 24);
        assertEquals(progressStats.getInputRows(), 124539);
        assertEquals(progressStats.getInputBytes(), 8283750);
        assertEquals(progressStats.isBlocked(), false);
        assertEquals(progressStats.getProgressPercentage(), OptionalDouble.empty());
    }
}
