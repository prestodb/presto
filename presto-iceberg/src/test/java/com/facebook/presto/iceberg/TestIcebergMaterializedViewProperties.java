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
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;

import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_MAX_SNAPSHOTS_PER_REFRESH;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.resolveMaxSnapshotsPerRefresh;
import static com.facebook.presto.iceberg.IcebergMaterializedViewProperties.MAX_SNAPSHOTS_PER_REFRESH;
import static com.facebook.presto.iceberg.IcebergMaterializedViewProperties.getMaxSnapshotsPerRefresh;
import static com.facebook.presto.iceberg.IcebergSessionProperties.MATERIALIZED_VIEW_DEFAULT_MAX_SNAPSHOTS_PER_REFRESH;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestIcebergMaterializedViewProperties
{
    private final IcebergMaterializedViewProperties properties = new IcebergMaterializedViewProperties(new IcebergTableProperties(new IcebergConfig()));

    @Test
    public void testAccessorReturnsEmptyWhenAbsent()
    {
        assertFalse(getMaxSnapshotsPerRefresh(ImmutableMap.of()).isPresent());
    }

    @Test
    public void testAccessorPassesThroughPositive()
    {
        Map<String, Object> map = new HashMap<>();
        map.put(MAX_SNAPSHOTS_PER_REFRESH, 25);
        assertEquals(getMaxSnapshotsPerRefresh(map), OptionalInt.of(25));
    }

    @Test
    public void testDecodeNullReturnsNull()
    {
        assertEquals(maxSnapshotsMetadata().decode(null), null);
    }

    @Test
    public void testDecodePositive()
    {
        assertEquals(maxSnapshotsMetadata().decode(42L), Integer.valueOf(42));
    }

    @Test
    public void testDecodeZeroRejected()
    {
        assertRejected(0L);
    }

    @Test
    public void testDecodeNegativeRejected()
    {
        assertRejected(-1L);
    }

    @Test
    public void testResolveReturnsEmptyWhenNothingSet()
    {
        assertEquals(resolveMaxSnapshotsPerRefresh(sessionWithDefault(0), ImmutableMap.of()), OptionalInt.empty());
    }

    @Test
    public void testResolveSessionDefault()
    {
        assertEquals(resolveMaxSnapshotsPerRefresh(sessionWithDefault(7), ImmutableMap.of()), OptionalInt.of(7));
    }

    @Test
    public void testResolveSessionDefaultZeroMeansUnbounded()
    {
        assertEquals(resolveMaxSnapshotsPerRefresh(sessionWithDefault(0), ImmutableMap.of()), OptionalInt.empty());
    }

    @Test
    public void testResolvePersistedOverridesSession()
    {
        assertEquals(
                resolveMaxSnapshotsPerRefresh(
                        sessionWithDefault(7),
                        ImmutableMap.of(PRESTO_MATERIALIZED_VIEW_MAX_SNAPSHOTS_PER_REFRESH, "3")),
                OptionalInt.of(3));
    }

    private void assertRejected(long value)
    {
        try {
            maxSnapshotsMetadata().decode(value);
            fail("expected PrestoException for value " + value);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), INVALID_TABLE_PROPERTY.toErrorCode());
            assertTrue(e.getMessage().contains(MAX_SNAPSHOTS_PER_REFRESH), e.getMessage());
        }
    }

    private PropertyMetadata<?> maxSnapshotsMetadata()
    {
        return properties.getMaterializedViewProperties().stream()
                .filter(metadata -> metadata.getName().equals(MAX_SNAPSHOTS_PER_REFRESH))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Property " + MAX_SNAPSHOTS_PER_REFRESH + " not found"));
    }

    private static ConnectorSession sessionWithDefault(int value)
    {
        PropertyMetadata<Integer> property = integerProperty(
                MATERIALIZED_VIEW_DEFAULT_MAX_SNAPSHOTS_PER_REFRESH,
                "test",
                0,
                false);
        return new TestingConnectorSession(
                ImmutableList.of(property),
                ImmutableMap.of(MATERIALIZED_VIEW_DEFAULT_MAX_SNAPSHOTS_PER_REFRESH, value));
    }
}
