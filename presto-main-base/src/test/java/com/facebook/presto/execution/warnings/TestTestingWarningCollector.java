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
package com.facebook.presto.execution.warnings;

import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.testing.TestingWarningCollector;
import com.facebook.presto.testing.TestingWarningCollectorConfig;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingWarningCollector.createTestWarning;
import static org.testng.Assert.assertEquals;

public class TestTestingWarningCollector
{
    @Test
    public void testAddWarnings()
    {
        TestingWarningCollector collector = new TestingWarningCollector(new WarningCollectorConfig(), new TestingWarningCollectorConfig().setAddWarnings(true));
        ImmutableList.Builder<PrestoWarning> expectedWarningsBuilder = ImmutableList.builder();
        expectedWarningsBuilder.add(createTestWarning(1));
        assertEquals(collector.getWarnings(), expectedWarningsBuilder.build());
    }
}
