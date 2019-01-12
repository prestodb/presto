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
package io.prestosql.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.GroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

public class GeometryStateFactory
        implements AccumulatorStateFactory<GeometryState>
{
    private static final long OGC_GEOMETRY_BASE_INSTANCE_SIZE = ClassLayout.parseClass(OGCGeometry.class).instanceSize();

    @Override
    public GeometryState createSingleState()
    {
        return new SingleGeometryState();
    }

    @Override
    public Class<? extends GeometryState> getSingleStateClass()
    {
        return SingleGeometryState.class;
    }

    @Override
    public GeometryState createGroupedState()
    {
        return new GroupedGeometryState();
    }

    @Override
    public Class<? extends GeometryState> getGroupedStateClass()
    {
        return GroupedGeometryState.class;
    }

    public static class GroupedGeometryState
            implements GeometryState, GroupedAccumulatorState
    {
        private long groupId;
        private ObjectBigArray<OGCGeometry> geometries = new ObjectBigArray<>();
        private long size;

        @Override
        public OGCGeometry getGeometry()
        {
            return geometries.get(groupId);
        }

        @Override
        public void setGeometry(OGCGeometry geometry)
        {
            OGCGeometry previousValue = this.geometries.get(groupId);
            size -= getGeometryMemorySize(previousValue);
            size += getGeometryMemorySize(geometry);
            this.geometries.set(groupId, geometry);
        }

        @Override
        public void ensureCapacity(long size)
        {
            geometries.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return size + geometries.sizeOf();
        }

        @Override
        public final void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }
    }

    // Do a best-effort attempt to estimate the memory size
    private static long getGeometryMemorySize(OGCGeometry geometry)
    {
        if (geometry == null) {
            return 0;
        }
        // Due to the following issue:
        // https://github.com/Esri/geometry-api-java/issues/192
        // We must check if the geometry is empty before calculating its size.  Once the issue is resolved
        // and we bring the fix into our codebase, we can remove this check.
        if (geometry.isEmpty()) {
            return OGC_GEOMETRY_BASE_INSTANCE_SIZE;
        }
        return geometry.estimateMemorySize();
    }

    public static class SingleGeometryState
            implements GeometryState
    {
        private OGCGeometry geometry;

        @Override
        public OGCGeometry getGeometry()
        {
            return geometry;
        }

        @Override
        public void setGeometry(OGCGeometry geometry)
        {
            this.geometry = geometry;
        }

        @Override
        public long getEstimatedSize()
        {
            return getGeometryMemorySize(geometry);
        }
    }
}
