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
package com.facebook.presto.execution.scheduler.nodeselection;

import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSplitAssigner;
import com.facebook.presto.spi.ttl.ConfidenceBasedTtlInfo;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;

public class TestSimpleTtlNodeSplitAssigner
{
    @Test
    public void testTtlComparison()
    {
        ConfidenceBasedTtlInfo confidenceBasedTtlInfo = ConfidenceBasedTtlInfo.getInfiniteTtl();
        Duration estimatedExecutionTime = new Duration(1, TimeUnit.HOURS);
        assertTrue(SimpleTtlNodeSplitAssigner.isTtlEnough(confidenceBasedTtlInfo, estimatedExecutionTime));
    }
}
