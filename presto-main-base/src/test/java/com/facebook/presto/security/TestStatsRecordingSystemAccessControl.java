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
package com.facebook.presto.security;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.SystemAccessControl;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;

import static com.facebook.presto.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.testng.Assert.assertEquals;

public class TestStatsRecordingSystemAccessControl
{
    public static final AccessControlContext CONTEXT = new AccessControlContext(new QueryId("query_id"), Optional.empty(), Collections.emptySet(), Optional.empty(), WarningCollector.NOOP, new RuntimeStats(), Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testEverythingDelegated()
    {
        assertAllMethodsOverridden(SystemAccessControl.class, StatsRecordingSystemAccessControl.class);
    }

    @Test
    public void testStatsRecording()
    {
        SystemAccessControl delegate = new AllowAllSystemAccessControl();
        StatsRecordingSystemAccessControl statsRecordingAccessControl = new StatsRecordingSystemAccessControl(delegate);

        assertEquals(statsRecordingAccessControl.getStats().getCheckCanAccessCatalog().getTime().getAllTime().getCount(), 0.0);

        statsRecordingAccessControl.checkCanAccessCatalog(null, CONTEXT, "test-catalog");

        assertEquals(statsRecordingAccessControl.getStats().getCheckCanAccessCatalog().getTime().getAllTime().getCount(), 1.0);
    }
}
