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
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import com.facebook.presto.spi.PrestoException;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import java.util.HashSet;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class GeometryUtils
{
    private static final CoordinateSequenceFactory COORDINATE_SEQUENCE_FACTORY = new PackedCoordinateSequenceFactory();
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(COORDINATE_SEQUENCE_FACTORY);

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

    /**
     * Copy of com.esri.core.geometry.Interop.translateToAVNaN
     * <p>
     * JtsGeometrySerde#serialize must serialize NaN's the same way ESRI library does to achieve binary compatibility
     */
    public static double translateToAVNaN(double n)
    {
        return (Double.isNaN(n)) ? -Double.MAX_VALUE : n;
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
     * If the geometry is empty, return a Retangle with NaN coordinates.
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
     * If the geometry is empty, return a Retangle with NaN coordinates.
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

    public static org.locationtech.jts.geom.Point makeJtsEmptyPoint()
    {
        return GEOMETRY_FACTORY.createPoint();
    }

    public static org.locationtech.jts.geom.Point makeJtsPoint(Coordinate coordinate)
    {
        return GEOMETRY_FACTORY.createPoint(coordinate);
    }
}
