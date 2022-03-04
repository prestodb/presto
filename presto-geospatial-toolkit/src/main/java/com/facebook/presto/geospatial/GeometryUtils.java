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

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MultiVertexGeometry;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.ogc.OGCConcreteGeometryCollection;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCGeometryCollection;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.locationtech.jts.operation.IsSimpleOp;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.locationtech.jts.operation.valid.TopologyValidationError;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class GeometryUtils
{
    private static final CoordinateSequenceFactory COORDINATE_SEQUENCE_FACTORY = new PackedCoordinateSequenceFactory();
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(COORDINATE_SEQUENCE_FACTORY);
    private static final Set<String> ATOMIC_GEOMETRY_TYPES = new HashSet<>();
    static {
        ATOMIC_GEOMETRY_TYPES.add("LineString");
        ATOMIC_GEOMETRY_TYPES.add("Polygon");
        ATOMIC_GEOMETRY_TYPES.add("Point");
    }

    private GeometryUtils() {}

    /**
     * Copy of com.esri.core.geometry.Interop.translateFromAVNaN
     * <p>
     * deserializeEnvelope needs to recognize custom NAN values generated by
     * ESRI's serialization of empty geometries.
     */
    private static double translateFromAVNaN(double n)
    {
        return n < -1.0E38D ? (0.0D / 0.0) : n;
    }

    public static boolean isEsriNaN(double d)
    {
        return Double.isNaN(d) || Double.isNaN(translateFromAVNaN(d));
    }

    public static int getPointCount(OGCGeometry ogcGeometry)
    {
        GeometryCursor cursor = ogcGeometry.getEsriGeometryCursor();
        int points = 0;
        while (true) {
            com.esri.core.geometry.Geometry geometry = cursor.next();
            if (geometry == null) {
                return points;
            }

            if (geometry.isEmpty()) {
                continue;
            }

            if (geometry instanceof Point) {
                points++;
            }
            else {
                points += ((MultiVertexGeometry) geometry).getPointCount();
            }
        }
    }

    public static Envelope getEnvelope(OGCGeometry ogcGeometry)
    {
        GeometryCursor cursor = ogcGeometry.getEsriGeometryCursor();
        Envelope overallEnvelope = new Envelope();
        while (true) {
            Geometry geometry = cursor.next();
            if (geometry == null) {
                return overallEnvelope;
            }

            Envelope envelope = new Envelope();
            geometry.queryEnvelope(envelope);
            overallEnvelope.merge(envelope);
        }
    }

    /**
     * Get the bounding box for an OGCGeometry.
     * <p>
     * If the geometry is empty, return a Rectangle with NaN coordinates.
     *
     * @param ogcGeometry
     * @return Rectangle bounding box
     */
    public static Rectangle getExtent(OGCGeometry ogcGeometry)
    {
        return getExtent(ogcGeometry, 0.0);
    }

    /**
     * Get the bounding box for an OGCGeometry, inflated by radius.
     * <p>
     * If the geometry is empty, return a Rectangle with NaN coordinates.
     *
     * @param ogcGeometry
     * @return Rectangle bounding box
     */
    public static Rectangle getExtent(OGCGeometry ogcGeometry, double radius)
    {
        com.esri.core.geometry.Envelope envelope = getEnvelope(ogcGeometry);

        return new Rectangle(
                envelope.getXMin() - radius,
                envelope.getYMin() - radius,
                envelope.getXMax() + radius,
                envelope.getYMax() + radius);
    }

    public static org.locationtech.jts.geom.Envelope getJtsEnvelope(OGCGeometry ogcGeometry, double radius)
    {
        Envelope esriEnvelope = getEnvelope(ogcGeometry);

        if (esriEnvelope.isEmpty()) {
            return new org.locationtech.jts.geom.Envelope();
        }

        return new org.locationtech.jts.geom.Envelope(
                esriEnvelope.getXMin() - radius,
                esriEnvelope.getXMax() + radius,
                esriEnvelope.getYMin() - radius,
                esriEnvelope.getYMax() + radius);
    }

    public static org.locationtech.jts.geom.Envelope getJtsEnvelope(OGCGeometry ogcGeometry)
    {
        return getJtsEnvelope(ogcGeometry, 0.0);
    }

    public static boolean disjoint(Envelope envelope, OGCGeometry ogcGeometry)
    {
        GeometryCursor cursor = ogcGeometry.getEsriGeometryCursor();
        while (true) {
            Geometry geometry = cursor.next();
            if (geometry == null) {
                return true;
            }

            if (!GeometryEngine.disjoint(geometry, envelope, null)) {
                return false;
            }
        }
    }

    public static boolean contains(OGCGeometry ogcGeometry, Envelope envelope)
    {
        GeometryCursor cursor = ogcGeometry.getEsriGeometryCursor();
        while (true) {
            Geometry geometry = cursor.next();
            if (geometry == null) {
                return false;
            }

            if (GeometryEngine.contains(geometry, envelope, null)) {
                return true;
            }
        }
    }

    public static boolean isPointOrRectangle(OGCGeometry ogcGeometry, Envelope envelope)
    {
        if (ogcGeometry instanceof OGCPoint) {
            return true;
        }

        if (!(ogcGeometry instanceof OGCPolygon)) {
            return false;
        }

        Polygon polygon = (Polygon) ogcGeometry.getEsriGeometry();
        if (polygon.getPathCount() > 1) {
            return false;
        }

        if (polygon.getPointCount() != 4) {
            return false;
        }

        Set<Point> corners = new HashSet<>();
        corners.add(new Point(envelope.getXMin(), envelope.getYMin()));
        corners.add(new Point(envelope.getXMin(), envelope.getYMax()));
        corners.add(new Point(envelope.getXMax(), envelope.getYMin()));
        corners.add(new Point(envelope.getXMax(), envelope.getYMax()));

        for (int i = 0; i < 4; i++) {
            Point point = polygon.getPoint(i);
            if (!corners.contains(point)) {
                return false;
            }
        }

        return true;
    }

    public static org.locationtech.jts.geom.Geometry jtsGeometryFromJson(String json)
    {
        try {
            return new GeoJsonReader().read(json);
        }
        catch (ParseException | IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid GeoJSON: " + e.getMessage(), e);
        }
    }

    public static Optional<String> jsonFromJtsGeometry(org.locationtech.jts.geom.Geometry geometry)
    {
        if (ATOMIC_GEOMETRY_TYPES.contains(geometry.getGeometryType()) && geometry.isEmpty()) {
            return Optional.empty();
        }
        else {
            return Optional.of(new GeoJsonWriter().write(geometry));
        }
    }

    public static org.locationtech.jts.geom.Geometry jtsGeometryFromWkt(String wkt)
    {
        try {
            return new WKTReader(GEOMETRY_FACTORY).read(wkt);
        }
        catch (ParseException | IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid WKT: " + e.getMessage(), e);
        }
    }

    public static String wktFromJtsGeometry(org.locationtech.jts.geom.Geometry geometry)
    {
        return new WKTWriter().write(geometry);
    }

    public static org.locationtech.jts.geom.Point createJtsEmptyPoint()
    {
        return GEOMETRY_FACTORY.createPoint();
    }

    public static org.locationtech.jts.geom.Point createJtsPoint(Coordinate coordinate)
    {
        return GEOMETRY_FACTORY.createPoint(coordinate);
    }

    public static org.locationtech.jts.geom.Point createJtsPoint(double x, double y)
    {
        return createJtsPoint(new Coordinate(x, y));
    }

    public static org.locationtech.jts.geom.MultiPoint createJtsMultiPoint(CoordinateSequence coordinates)
    {
        return GEOMETRY_FACTORY.createMultiPoint(coordinates);
    }

    public static org.locationtech.jts.geom.Geometry createJtsEmptyLineString()
    {
        return GEOMETRY_FACTORY.createLineString();
    }

    public static org.locationtech.jts.geom.Geometry createJtsLineString(CoordinateSequence coordinates)
    {
        return GEOMETRY_FACTORY.createLineString(coordinates);
    }

    public static org.locationtech.jts.geom.Geometry createJtsEmptyPolygon()
    {
        return GEOMETRY_FACTORY.createPolygon();
    }

    public static Optional<String> getGeometryInvalidReason(org.locationtech.jts.geom.Geometry geometry)
    {
        IsValidOp validOp = new IsValidOp(geometry);
        IsSimpleOp simpleOp = new IsSimpleOp(geometry);
        try {
            TopologyValidationError err = validOp.getValidationError();
            if (err != null) {
                return Optional.of(err.getMessage());
            }
        }
        catch (UnsupportedOperationException e) {
            // This is thrown if the type of geometry is unsupported by JTS.
            // It should not happen in practice.
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Geometry type not valid", e);
        }
        if (!simpleOp.isSimple()) {
            String errorDescription;
            String geometryType = geometry.getGeometryType();
            switch (GeometryType.getForJtsGeometryType(geometryType)) {
                case POINT:
                    errorDescription = "Invalid point";
                    break;
                case MULTI_POINT:
                    errorDescription = "Repeated point";
                    break;
                case LINE_STRING:
                case MULTI_LINE_STRING:
                    errorDescription = "Self-intersection at or near";
                    break;
                case POLYGON:
                case MULTI_POLYGON:
                case GEOMETRY_COLLECTION:
                    // In OGC (which JTS follows): Polygons, MultiPolygons, Geometry Collections are simple.
                    // This shouldn't happen, but in case it does, return a reasonable generic message.
                    errorDescription = "Topology exception at or near";
                    break;
                default:
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unknown geometry type: %s", geometryType));
            }
            org.locationtech.jts.geom.Coordinate nonSimpleLocation = simpleOp.getNonSimpleLocation();
            return Optional.of(format("[%s] %s: (%s %s)", geometryType, errorDescription, nonSimpleLocation.getX(), nonSimpleLocation.getY()));
        }
        return Optional.empty();
    }

    /**
     * Recursively flatten GeometryCollection in geometry.
     *
     * If `geometry` is null, return an empty iterable.
     * If `geometry` is not a GeometryCollection, yield a single geometry.
     * (For this, MultiX is not considered a GeometryCollection.)
     * Otherwise, iterate through all children of the GeometryCollection,
     * recursively flattening contained GeometryCollections.
     */
    public static Iterable<OGCGeometry> flattenCollection(OGCGeometry geometry)
    {
        if (geometry == null) {
            return ImmutableList.of();
        }
        if (!(geometry instanceof OGCConcreteGeometryCollection)) {
            return ImmutableList.of(geometry);
        }
        if (((OGCConcreteGeometryCollection) geometry).numGeometries() == 0) {
            return ImmutableList.of();
        }
        return () -> new GeometryCollectionIterator(geometry);
    }

    public static void accelerateGeometry(OGCGeometry ogcGeometry, Operator relateOperator)
    {
        accelerateGeometry(ogcGeometry, relateOperator, Geometry.GeometryAccelerationDegree.enumMild);
    }

    public static void accelerateGeometry(
            OGCGeometry ogcGeometry,
            Operator relateOperator,
            Geometry.GeometryAccelerationDegree accelerationDegree)
    {
        // Recurse into GeometryCollections
        GeometryCursor cursor = ogcGeometry.getEsriGeometryCursor();
        while (true) {
            Geometry esriGeometry = cursor.next();
            if (esriGeometry == null) {
                break;
            }
            relateOperator.accelerateGeometry(esriGeometry, null, accelerationDegree);
        }
    }

    private static class GeometryCollectionIterator
            implements Iterator<OGCGeometry>
    {
        private final Deque<OGCGeometry> geometriesDeque = new ArrayDeque<>();

        GeometryCollectionIterator(OGCGeometry geometries)
        {
            geometriesDeque.push(requireNonNull(geometries, "geometries is null"));
        }

        @Override
        public boolean hasNext()
        {
            if (geometriesDeque.isEmpty()) {
                return false;
            }
            while (geometriesDeque.peek() instanceof OGCConcreteGeometryCollection) {
                OGCGeometryCollection collection = (OGCGeometryCollection) geometriesDeque.pop();
                for (int i = 0; i < collection.numGeometries(); i++) {
                    geometriesDeque.push(collection.geometryN(i));
                }
            }
            return !geometriesDeque.isEmpty();
        }

        @Override
        public OGCGeometry next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException("Geometries have been consumed");
            }
            return geometriesDeque.pop();
        }
    }
}
