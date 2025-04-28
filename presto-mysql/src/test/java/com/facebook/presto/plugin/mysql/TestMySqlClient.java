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
package com.facebook.presto.plugin.mysql;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import com.facebook.presto.plugin.jdbc.SliceReadFunction;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.h2.tools.SimpleResultSet;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.sql.SQLException;

import static com.facebook.presto.geospatial.GeoFunctions.stGeomFromBinary;
import static com.facebook.presto.plugin.mysql.MySqlClient.geometryReadMapping;
import static com.facebook.presto.plugin.mysql.MySqlClient.getAsText;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestMySqlClient
{
    @Test
    public void testValidGeometryReadMapping()
    {
        OGCGeometry geometry = new OGCPoint(new Point(1.0, 2.0), null);
        ByteBuffer buffer = geometry.asBinary();

        Slice value = getAsText(stGeomFromBinary(Slices.wrappedBuffer(buffer)));

        assertEquals(value.toStringUtf8(), "POINT (1 2)");
    }

    @Test
    public void testInvalidGeometryReadMapping()
    {
        byte[] invalidWkb = new byte[]{0x00, 0x01, 0x02};
        try {
            getAsText(stGeomFromBinary(Slices.wrappedBuffer(invalidWkb)));
            fail("stGeomFromBinary should throw");
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), "Invalid WKB");
        }
    }

    @Test
    public void testReadMapping() throws SQLException
    {
        SliceReadFunction fn = (SliceReadFunction) geometryReadMapping().getReadFunction();
        OGCGeometry geometry = new OGCPoint(new Point(1.0, 2.0), null);
        ByteBuffer buffer = geometry.asBinary();
        Slice value = fn.readSlice(new MockResultSet(buffer.array()), 1);
        assertEquals(value.toStringUtf8(), "POINT (1 2)");

        byte[] invalid = new byte[] {0x01, 0x02, 0x03};
        try {
            fn.readSlice(new MockResultSet(invalid), 1);
            fail("stGeomFromBinary should throw");
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), "Invalid Well-Known Binary (WKB)");
        }
    }

    private static class MockResultSet
            extends SimpleResultSet
    {
        private final byte[] bytes;

        public MockResultSet(byte[] bytes)
        {
            this.bytes = bytes;
        }

        @Override
        public byte[] getBytes(int columnIndex)
        {
            return bytes;
        }
    }
}
