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
package com.facebook.presto.delta;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeltaSessionProperties
{
    @Test
    public void testDeletionVectorsEnabledSessionPropertyDefault()
    {
        DeltaConfig config = new DeltaConfig();
        DeltaSessionProperties sessionProperties = new DeltaSessionProperties(config, new CacheConfig());

        // When no explicit session property is set, the session should use the connector default
        ConnectorSession session = new TestingConnectorSession(sessionProperties.getSessionProperties());

        assertEquals(
                DeltaSessionProperties.isDeletionVectorsEnabled(session),
                config.isDeletionVectorsEnabled(),
                "Session property should default to connector config value for deletion vectors");
    }

    @Test
    public void testDeletionVectorsEnabledSessionPropertyOverride()
    {
        DeltaConfig config = new DeltaConfig();
        DeltaSessionProperties sessionProperties = new DeltaSessionProperties(config, new CacheConfig());

        // Explicitly disable deletion vectors in the session
        ConnectorSession sessionDisabled = new TestingConnectorSession(
                sessionProperties.getSessionProperties(),
                ImmutableMap.of(DeltaSessionProperties.DELETION_VECTORS_ENABLED, false));

        assertFalse(
                DeltaSessionProperties.isDeletionVectorsEnabled(sessionDisabled),
                "Session property should override connector config and disable deletion vectors");

        // Explicitly enable deletion vectors in the session
        ConnectorSession sessionEnabled = new TestingConnectorSession(
                sessionProperties.getSessionProperties(),
                ImmutableMap.of(DeltaSessionProperties.DELETION_VECTORS_ENABLED, true));

        assertTrue(
                DeltaSessionProperties.isDeletionVectorsEnabled(sessionEnabled),
                "Session property should override connector config and enable deletion vectors");
    }
}
