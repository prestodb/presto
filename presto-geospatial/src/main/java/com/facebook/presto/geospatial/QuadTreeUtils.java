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
package com.facebook.presto.geospatial;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.spi.PrestoException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class QuadTreeUtils
{
    private static final int MAX_QUAD_TREE_LEVEL = 16;

    private QuadTreeUtils() {}

    public static GeoIndex init(Map<String, OGCGeometry> geoshapes)
    {
        int id = 0;
        Map<Integer, GeoItem> indexMap = new HashMap<>();
        Map<Integer, Envelope2D> envelope2DMap = new HashMap<>();
        Envelope2D extent = new Envelope2D();

        for (Map.Entry<String, OGCGeometry> shape : geoshapes.entrySet()) {
            String indexField = shape.getKey();
            OGCGeometry geometry = shape.getValue();

            Envelope2D envelope = new Envelope2D();
            geometry.getEsriGeometry().queryEnvelope2D(envelope);

            envelope2DMap.put(id, envelope);
            extent.merge(envelope);

            indexMap.put(id, new GeoItem(indexField, geometry));
            id++;
        }
        // Init quad tree with simplified envelope2D
        QuadTree quadTree = new QuadTree(extent, MAX_QUAD_TREE_LEVEL);
        for (Map.Entry<Integer, Envelope2D> entry : envelope2DMap.entrySet()) {
            quadTree.insert(entry.getKey(), entry.getValue());
        }

        return new GeoIndex(indexMap, quadTree);
    }

    public static List<String> queryIndex(OGCGeometry shape, GeoIndex geoIndex, OperatorSimpleRelation relationOperator, boolean getFirst)
            throws Exception
    {
        QuadTree quadTree = geoIndex.quadTree;
        Map<Integer, GeoItem> indexMap = geoIndex.geoIndex;
        List<Integer> indexCandidates = new ArrayList<>();
        List<String> shapeIds = new ArrayList<>();

        try {
            Geometry esriShape = shape.getEsriGeometry();
            QuadTree.QuadTreeIterator iter = quadTree.getIterator(esriShape, 0);
            for (int handle = iter.next(); handle != -1; handle = iter.next()) {
                int element = quadTree.getElement(handle);
                indexCandidates.add(element);
            }

            for (int indexCandidate : indexCandidates) {
                String indexField = indexMap.get(indexCandidate).indexField;
                OGCGeometry geometry = indexMap.get(indexCandidate).geometry;
                if (relationOperator.execute(geometry.getEsriGeometry(), esriShape, geometry.getEsriSpatialReference(), null)) {
                    shapeIds.add(indexField);
                    if (getFirst) {
                        break;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error in query index", e);
        }
        return shapeIds;
    }

    public static class GeoIndex
    {
        public Map<Integer, GeoItem> geoIndex;
        public QuadTree quadTree;

        public GeoIndex(Map<Integer, GeoItem> geoIndex, QuadTree quadTree)
        {
            this.geoIndex = geoIndex;
            this.quadTree = quadTree;
        }
    }

    public static class GeoItem
    {
        String indexField;
        OGCGeometry geometry;

        GeoItem(String indexField, OGCGeometry geometry)
        {
            this.indexField = indexField;
            this.geometry = geometry;
        }
    }
}
