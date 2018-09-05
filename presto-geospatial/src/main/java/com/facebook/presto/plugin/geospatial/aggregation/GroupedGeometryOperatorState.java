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

import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.ListeningGeometryCursor;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.GroupedAccumulatorState;

import static com.facebook.presto.plugin.geospatial.aggregation.GeometryCursorUtils.processGeometry;

public class GroupedGeometryOperatorState
        implements GeometryOperatorState.GeometryUnionOperatorState, GeometryOperatorState.GeometryConvexHullOperatorState, GroupedAccumulatorState
{
    private final GeometryOperatorFactory operatorFactory;
    private long groupId;
    private ObjectBigArray<GeometryCursor> operators = new ObjectBigArray<>();
    private ObjectBigArray<ListeningGeometryCursor> inputs = new ObjectBigArray<>();
    private SpatialReference spatialReference;

    public GroupedGeometryOperatorState(GeometryOperatorFactory operatorFactory) {
        this.operatorFactory = operatorFactory;
    }

    @Override
    public OGCGeometry getGeometry()
    {
        GeometryCursor gc = operators.get(groupId);
        if (gc == null) {
            return null;
        }
        return OGCGeometry.createFromEsriCursor(gc, spatialReference);
    }

    @Override
    public void add(OGCGeometry geometry)
    {
        if (geometry == null || geometry.isEmpty()) {
            return;
        }
        if (spatialReference == null) {
            spatialReference = geometry.getEsriSpatialReference();
        }
        GeometryCursor operator = operators.get(groupId);
        ListeningGeometryCursor input;
        if (operator == null) {
            input = new ListeningGeometryCursor();
            operator = operatorFactory.create(input, spatialReference);
            inputs.set(groupId, input);
            operators.set(groupId, operator);
        }
        else {
            input = inputs.get(groupId);
        }
        processGeometry(geometry, input, operator);
    }

    @Override
    public void ensureCapacity(long size)
    {
        operators.ensureCapacity(size);
        inputs.ensureCapacity(size);
    }

    @Override
    public long getEstimatedSize()
    {
        return 0; // FIXME: missing logic to retrieve the estimated memory size of cursors
    }

    @Override
    public final void setGroupId(long groupId)
    {
        this.groupId = groupId;
    }
}
