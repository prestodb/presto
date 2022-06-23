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
import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;

public class GeometryStateFactory
        implements AccumulatorStateFactory<GeometryState>
{
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
        public void setGeometry(OGCGeometry geometry, long previousMemorySize)
        {
            this.geometries.set(groupId, geometry);
            size -= previousMemorySize;
            if (geometry != null) {
                size += geometry.estimateMemorySize();
            }
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
        public void setGeometry(OGCGeometry geometry, long previousMemorySize)
        {
            this.geometry = geometry;
        }

        @Override
        public long getEstimatedSize()
        {
            return geometry != null ? geometry.estimateMemorySize() : 0;
        }
    }
}
