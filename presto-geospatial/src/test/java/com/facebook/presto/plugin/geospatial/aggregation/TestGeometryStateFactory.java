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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestGeometryStateFactory
{
    private GeometryStateFactory factory;

    @BeforeMethod
    public void init()
    {
        factory = new GeometryStateFactory();
    }

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
        state.setGeometry(OGCGeometry.fromText("POINT (1 2)"));
        assertEquals(OGCGeometry.fromText("POINT (1 2)"), state.getGeometry());
        assertTrue(state.getEstimatedSize() > 0, String.format("Estimated memory size was %d", state.getEstimatedSize()));
    }

    @Test
    public void testCreateGroupedStateEmpty()
    {
        GeometryState state = factory.createGroupedState();
        assertNull(state.getGeometry());
        assertTrue(state.getEstimatedSize() > 0, String.format("Estimated memory size was %d", state.getEstimatedSize()));
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
        groupedState.setGeometry(OGCGeometry.fromText("POINT (1 2)"));
        assertEquals(state.getGeometry(), OGCGeometry.fromText("POINT (1 2)"));

        groupedState.setGroupId(2);
        assertNull(state.getGeometry());
        groupedState.setGeometry(OGCGeometry.fromText("POINT (3 4)"));
        assertEquals(state.getGeometry(), OGCGeometry.fromText("POINT (3 4)"));

        groupedState.setGroupId(1);
        assertNotNull(state.getGeometry());
    }
}
