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
package com.facebook.presto.runtimestats;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.TestRuntimeStatsInstrument;
import com.facebook.presto.spi.TestRuntimeStatsInstrumentFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestRuntimeStatsManager
{
    private RuntimeStatsManager runtimeStatsManager;

    @BeforeClass
    public void setup()
    {
        runtimeStatsManager = new RuntimeStatsManager();
    }

    @Test
    public void testStaticNewRuntimeStats()
    {
        RuntimeStats runtimeStats = RuntimeStatsManager.newRuntimeStats();
        assertNotNull(runtimeStats);
        assertTrue(runtimeStats.getRuntimeStatsInstruments().isEmpty());
    }

    @Test
    public void testNewRuntimeStatsWithInstruments()
    {
        // case 1: No instrument factory added.
        RuntimeStats runtimeStats1 = runtimeStatsManager.newRuntimeStatsWithInstruments();
        assertNotNull(runtimeStats1);
        assertTrue(runtimeStats1.getRuntimeStatsInstruments().isEmpty());

        // case 2: Instrument factory registered.
        TestRuntimeStatsInstrumentFactory testRuntimeStatsInstrumentFactory = new TestRuntimeStatsInstrumentFactory();
        runtimeStatsManager.addRuntimeStatsInstrumentFactory(testRuntimeStatsInstrumentFactory);
        RuntimeStats runtimeStats2 = runtimeStatsManager.newRuntimeStatsWithInstruments();
        assertNotNull(runtimeStats2);
        assertTrue(runtimeStats2.getRuntimeStatsInstruments().isEmpty());

        // case 3: Load instrument factory.
        runtimeStatsManager.loadRuntimeStatsInstruments();
        RuntimeStats runtimeStats3 = runtimeStatsManager.newRuntimeStatsWithInstruments();
        assertNotNull(runtimeStats3);
        assertFalse(runtimeStats3.getRuntimeStatsInstruments().isEmpty());
        assertEquals(runtimeStats3.getRuntimeStatsInstruments().size(), 1);
        assertTrue(runtimeStats3.getRuntimeStatsInstruments().get(0) instanceof TestRuntimeStatsInstrument);
    }
}
