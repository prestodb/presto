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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * The position-aware {@code addColumn} overload has a default implementation that keeps connectors
 * which only implement the older three-argument form working unchanged.
 */
public class TestConnectorMetadataAddColumn
{
    private static final ColumnMetadata COLUMN = ColumnMetadata.builder().setName("c").setType(BIGINT).build();

    @Test
    public void testLastDelegatesToLegacyOverload()
    {
        LegacyMetadata metadata = new LegacyMetadata();
        metadata.addColumn(null, null, COLUMN, new ColumnPosition.Last());
        assertEquals(metadata.getLegacyCallCount(), 1);
    }

    @Test
    public void testFirstIsRejected()
    {
        LegacyMetadata metadata = new LegacyMetadata();
        assertNotSupported(metadata, new ColumnPosition.First(), "This connector does not support adding columns with FIRST clause");
        // The legacy overload must not be reached, or the column would be appended in the wrong place
        assertEquals(metadata.getLegacyCallCount(), 0);
    }

    @Test
    public void testAfterIsRejected()
    {
        LegacyMetadata metadata = new LegacyMetadata();
        assertNotSupported(metadata, new ColumnPosition.After("x"), "This connector does not support adding columns with AFTER clause");
        assertEquals(metadata.getLegacyCallCount(), 0);
    }

    @Test
    public void testConnectorWithoutAddColumnSupport()
    {
        // A connector that implements neither overload still reports the pre-existing message for LAST
        LegacyMetadata metadata = LegacyMetadata.unsupported();
        assertNotSupported(metadata, new ColumnPosition.Last(), "This connector does not support adding columns");
        // LAST still routes through the legacy overload, which is where that pre-existing rejection comes from
        assertEquals(metadata.getLegacyCallCount(), 1);
    }

    @Test
    public void testUnrecognizedPositionIsRejected()
    {
        // A position this default does not understand must not silently append, or the column lands in the wrong place
        LegacyMetadata metadata = new LegacyMetadata();
        assertNotSupported(metadata, new UnknownPosition(), "This connector does not support adding columns at position");
        assertEquals(metadata.getLegacyCallCount(), 0);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullPositionIsRejected()
    {
        new LegacyMetadata().addColumn(null, null, COLUMN, null);
    }

    private static void assertNotSupported(ConnectorMetadata metadata, ColumnPosition position, String message)
    {
        try {
            metadata.addColumn(null, null, COLUMN, position);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertTrue(e.getMessage().contains(message), "unexpected message: " + e.getMessage());
        }
    }

    /**
     * Stands in for a position type added to the SPI after this default was written.
     */
    private static class UnknownPosition
            implements ColumnPosition
    {
    }

    /**
     * A connector that implements only the older three-argument {@code addColumn}, counting the calls
     * it receives. {@link #unsupported()} instead stands in for a connector that implements neither
     * overload, by falling back to the interface default. Beyond {@code addColumn}, only the methods
     * {@link ConnectorMetadata} leaves abstract are stubbed out.
     */
    private static class LegacyMetadata
            implements ConnectorMetadata
    {
        private final boolean supported;
        private int legacyCallCount;

        LegacyMetadata()
        {
            this(true);
        }

        private LegacyMetadata(boolean supported)
        {
            this.supported = supported;
        }

        static LegacyMetadata unsupported()
        {
            return new LegacyMetadata(false);
        }

        @Override
        public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
        {
            // Counted before the delegation below, so the count records that this overload was entered even
            // when the unsupported case throws out of it
            legacyCallCount++;
            if (!supported) {
                ConnectorMetadata.super.addColumn(session, tableHandle, column);
            }
        }

        public int getLegacyCallCount()
        {
            return legacyCallCount;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
            return emptyList();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
        {
            return emptyMap();
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
        {
            return emptyMap();
        }
    }
}
