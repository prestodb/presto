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

import static com.facebook.presto.plugin.geospatial.aggregation.GeometryCursorUtils.processGeometry;

public class SingleGeometryOperatorState
        implements GeometryOperatorState.GeometryUnionOperatorState, GeometryOperatorState.GeometryConvexHullOperatorState
{
    private final ListeningGeometryCursor input;
    private final GeometryOperatorFactory operatorFactory;
    private GeometryCursor operator;
    private SpatialReference spatialReference;

    public SingleGeometryOperatorState(GeometryOperatorFactory operatorFactory)
    {
        this.input = new ListeningGeometryCursor();
        this.operatorFactory = operatorFactory;
    }

    @Override
    public OGCGeometry getGeometry()
    {
        if (operator == null) {
            return null;
        }
        return OGCGeometry.createFromEsriCursor(operator, spatialReference);
    }

    @Override
    public void add(OGCGeometry geometry)
    {
        if (geometry == null || geometry.isEmpty()) {
            return;
        }
        if (operator == null) {
            spatialReference = geometry.getEsriSpatialReference();
            operator = operatorFactory.create(input, spatialReference);
        }
        processGeometry(geometry, input, operator);
    }

    @Override
    public long getEstimatedSize()
    {
        return 0; // FIXME: missing logic to retrieve the estimated memory size of cursors
    }
}
