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
package com.facebook.presto.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestGeometryStateFactory
{
    private GeometryStateFactory factory = new GeometryStateFactory();

    @Test
    public void testCreateSingleStateEmpty()
    {
        GeometryState state = factory.createSingleState();
        assertNull(state.getGeometry());
        assertEquals(0, state.getEstimatedSize());
    }

    @Test
    public void testCreateSingleStatePresent()
    {
        GeometryState state = factory.createSingleState();
        state.setGeometry(OGCGeometry.fromText("POINT (1 2)"), 0);
        assertEquals(OGCGeometry.fromText("POINT (1 2)"), state.getGeometry());
        assertTrue(state.getEstimatedSize() > 0, format("Estimated memory size was %d", state.getEstimatedSize()));
    }

    @Test
    public void testCreateGroupedStateEmpty()
    {
        GeometryState state = factory.createGroupedState();
        assertNull(state.getGeometry());
        assertTrue(state.getEstimatedSize() > 0, format("Estimated memory size was %d", state.getEstimatedSize()));
    }

    @Test
    public void testCreateGroupedStatePresent()
    {
        GeometryState state = factory.createGroupedState();
        assertNull(state.getGeometry());
        assertTrue(state instanceof GeometryStateFactory.GroupedGeometryState);
        GeometryStateFactory.GroupedGeometryState groupedState = (GeometryStateFactory.GroupedGeometryState) state;

        groupedState.setGroupId(1);
        assertNull(state.getGeometry());
        groupedState.setGeometry(OGCGeometry.fromText("POINT (1 2)"), 0);
        assertEquals(state.getGeometry(), OGCGeometry.fromText("POINT (1 2)"));

        groupedState.setGroupId(2);
        assertNull(state.getGeometry());
        groupedState.setGeometry(OGCGeometry.fromText("POINT (3 4)"), 0);
        assertEquals(state.getGeometry(), OGCGeometry.fromText("POINT (3 4)"));

        groupedState.setGroupId(1);
        assertNotNull(state.getGeometry());
    }

    @Test
    public void testMemoryAccounting()
    {
        GeometryState state = factory.createGroupedState();
        long oldSize = state.getEstimatedSize();
        OGCGeometry geometry = OGCGeometry.fromText("POLYGON ((2 2, 1 1, 3 1, 2 2))");

        long previousGeometryMemorySize = 0;
        state.setGeometry(geometry, previousGeometryMemorySize);
        assertTrue(oldSize < state.getEstimatedSize(), format("Expected old size %s to be less than new estimate %s", oldSize, state.getEstimatedSize()));

        oldSize = state.getEstimatedSize();
        previousGeometryMemorySize = state.getGeometry().estimateMemorySize();
        state.setGeometry(state.getGeometry().union(geometry), previousGeometryMemorySize);
        assertTrue(oldSize <= state.getEstimatedSize(), format("Expected old size %s to be less than or equal to new estimate %s", oldSize, state.getEstimatedSize()));
    }
}
