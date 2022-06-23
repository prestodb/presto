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
package com.facebook.presto.execution;

import com.facebook.presto.execution.scheduler.PartialResultQueryTaskTracker;
import com.facebook.presto.execution.warnings.DefaultWarningCollector;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.spi.WarningCollector;
import org.testng.annotations.Test;

import static com.facebook.presto.execution.warnings.WarningHandlingLevel.NORMAL;
import static org.testng.Assert.assertEquals;

public class TestPartialResultQueryManager
{
    private final WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig(), NORMAL);

    @Test
    public void testPartialResultQueryManager()
            throws Exception
    {
        PartialResultQueryManager partialResultQueryManager = new PartialResultQueryManager();
        assertEquals(0, partialResultQueryManager.getQueueSize());

        PartialResultQueryTaskTracker tracker1 = new PartialResultQueryTaskTracker(partialResultQueryManager, 0.0, 2.0, warningCollector);
        PartialResultQueryTaskTracker tracker2 = new PartialResultQueryTaskTracker(partialResultQueryManager, 0.0, 2.0, warningCollector);

        // Assert that the trackers created above will have a default maxEndTime = 0. So current_time is always > tracker's maxEndTime.Meaning tracker is instantly ready for partial results.
        assertEquals(0, tracker1.getMaxEndTime());
        assertEquals(0, tracker2.getMaxEndTime());

        partialResultQueryManager.addQueryTaskTracker(tracker1);
        partialResultQueryManager.addQueryTaskTracker(tracker2);

        // Assert that the trackers are added to the queue
        assertEquals(2, partialResultQueryManager.getQueueSize());

        // Sleep for 2s so that we give enough time to partialResultQueryManager to wake up and clear the trackers in queue
        Thread.sleep(2000);

        // Assert the trackers are cleared
        assertEquals(0, partialResultQueryManager.getQueueSize());

        partialResultQueryManager.stop();
    }
}
