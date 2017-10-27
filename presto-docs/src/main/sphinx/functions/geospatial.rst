====================
Geospatial Functions
====================

Presto Geospatial functions support the SQL/MM specification.
They are compliant with the Open Geospatial Consortium’s (OGC) OpenGIS Specifications.
As such, many Presto Geospatial functions require, or more accurately, assume that
geometries that are operated on are both simple and valid. For example, it does not
make sense to calculate the area of a polygon that has a hole defined outside of the
polygon, or to construct a polygon from a non-simple boundary line.

Presto Geospatial functions support the Well-Known Text (WKT) form of spatial objects:

* ``POINT (0 0)``
* ``LINESTRING (0 0, 1 1, 1 2)``
* ``POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))``
* ``MULTIPOINT (0 0, 1 2)``
* ``MULTILINESTRING ((0 0, 1 1, 1 2), (2 3, 3 2, 5 4))``
* ``MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)), ((-1 -1, -1 -2, -2 -2, -2 -1, -1 -1)))``
* ``GEOMETRYCOLLECTION (POINT(2 3), LINESTRING (2 3, 3 4))``

Constructors
------------

.. function:: ST_Point(double, double) -> Point

    Returns a geometry type point object with the given coordinate values.

.. function:: ST_LineFromText(varchar) -> LineString

    Returns a geometry type linestring object from WKT representation.

.. function:: ST_Polygon(varchar) -> Polygon

    Returns a geometry type polygon object from WKT representation.

.. function:: ST_GeometryFromText(varchar) -> Geometry

    Returns a geometry type object from WKT representation.

.. function:: ST_AsText(Geometry) -> varchar

    Returns the WKT representation of the geometry. For empty geometries,
    ``ST_AsText(ST_LineFromText('LINESTRING EMPTY'))`` will produce ``'MULTILINESTRING EMPTY'``
    and ``ST_AsText(ST_Polygon('POLYGON EMPTY'))`` will produce ``'MULTIPOLYGON EMPTY'``.

Relationship Tests
------------------

.. function:: ST_Contains(Geometry, Geometry) -> boolean

    Returns ``true`` if and only if no points of the second geometry lie in the exterior
    of the first geometry, and at least one point of the interior of the first geometry
    lies in the interior of the second geometry.

.. function:: ST_Crosses(Geometry, Geometry) -> boolean

    Returns ``true`` if the supplied geometries have some, but not all, interior points in common.

.. function:: ST_Disjoint(Geometry, Geometry) -> boolean

    Returns ``true`` if the give geometries do not *spatially intersect* --
    if they do not share any space together.

.. function:: ST_Equals(Geometry, Geometry) -> boolean

    Returns ``true`` if the given geometries represent the same geometry.

.. function:: ST_Intersects(Geometry, Geometry) -> boolean

    Returns ``true`` if the given geometries spatially intersect in two dimensions
    (share any portion of space) and ``false`` if they don not (they are disjoint).

.. function:: ST_Overlaps(Geometry, Geometry) -> boolean

    Returns ``true`` if the given geometries share space, are of the same dimension,
    but are not completely contained by each other.

.. function:: ST_Relate(Geometry, Geometry) -> boolean

    Returns ``true`` if first geometry is spatially related to second geometry.

.. function:: ST_Touches(Geometry, Geometry) -> boolean

    Returns ``true`` if the given geometries have at least one point in common,
    but their interiors do not intersect.

.. function:: ST_Within(Geometry, Geometry) -> boolean

    Returns ``true`` if first geometry is completely inside second geometry.

Operations
----------

.. function:: ST_Boundary(Geometry) -> Geometry

    Returns the closure of the combinatorial boundary of this geometry.

.. function:: ST_Difference(Geometry, Geometry) -> Geometry

    Returns the geometry value that represents the point set difference of the given geometries.

.. function:: ST_Envelope(Geometry) -> Geometry

    Returns the bounding rectangular polygon of a geometry.

.. function:: ST_ExteriorRing(Geometry) -> Geometry

    Returns a line string representing the exterior ring of the input polygon.

.. function:: ST_Intersection(Geometry, Geometry) -> Geometry

    Returns the geometry value that represents the point set intersection of two geometries.

.. function:: ST_SymDifference(Geometry, Geometry) -> Geometry

    Returns the geometry value that represents the point set symmetric difference of two geometries.

Accessors
---------

.. function:: ST_Area(Geometry) -> double

    Returns the area of a polygon using Euclidean measurement on a two dimensional
    plane (based on spatial ref) in projected units.

.. function:: ST_Centroid(Geometry) -> Geometry

    Returns the point value that is the mathematical centroid of a geometry.

.. function:: ST_CoordDim(Geometry) -> bigint

    Return the coordinate dimension of the geometry.

.. function:: ST_Dimension(Geometry) -> bigint

    Returns the inherent dimension of this geometry object, which must be
    less than or equal to the coordinate dimension.

.. function:: ST_Distance(Geometry, Geometry) -> double

    Returns the 2-dimensional cartesian minimum distance (based on spatial ref)
    between two geometries in projected units.

.. function:: ST_IsClosed(Geometry) -> boolean

    Returns ``true`` if the linestring's start and end points are coincident.

.. function:: ST_IsEmpty(Geometry) -> boolean

    Returns ``true`` if this Geometry is an empty geometrycollection, polygon, point etc.

.. function:: ST_IsRing(Geometry) -> boolean

    Returns ``true`` if and only if the line is closed and simple.

.. function:: ST_Length(Geometry) -> double

    Returns the length of a linestring or multi-linestring using Euclidean measurement on a
    two dimensional plane (based on spatial ref) in projected units.

.. function:: ST_XMax(Geometry) -> double

    Returns X maxima of a bounding box of a geometry.

.. function:: ST_YMax(Geometry) -> double

    Returns Y maxima of a bounding box of a geometry.

.. function:: ST_XMin(Geometry) -> double

    Returns X minima of a bounding box of a geometry.

.. function:: ST_YMin(Geometry) -> double

    Returns Y minima of a bounding box of a geometry.

.. function:: ST_StartPoint(Geometry) -> point

    Returns the first point of a LineString geometry as a Point.

.. function:: ST_EndPoint(Geometry) -> point

    Returns the last point of a LineString geometry as a Point.

.. function:: ST_X(Point) -> double

    Return the X coordinate of the point.

.. function:: ST_Y(Point) -> double

    Return the Y coordinate of the point.

.. function:: ST_NumPoints(Geometry) -> bigint

    Returns the number of points in a geometry. This is an extension to the SQL/MM
    ``ST_NumPoints`` function which only applies to point and linestring.

.. function:: ST_NumInteriorRing(Geometry) -> bigint

    Returns the cardinality of the collection of interior rings of a polygon.

Bing Tiles
----------

These functions convert between geometries and
`Bing tiles <https://msdn.microsoft.com/en-us/library/bb259689.aspx>`_.

.. function:: bing_tile(x, y, zoom_level) -> BingTile

    Creates a Bing tile object from XY coordinates and a zoom level.
    Zoom levels from 1 to 23 are supported.

.. function:: bing_tile(quadKey) -> BingTile

    Creates a Bing tile object from a quadkey.

.. function:: bing_tile_at(latitude, longitude, zoom_level) -> BingTile

    Returns a Bing tile at a given zoom level containing a point at a given latitude
    and longitude. Latitude must be within ``[-85.05112878, 85.05112878]`` range.
    Longitude must be within ``[-180, 180]`` range. Zoom levels from 1 to 23 are supported.

.. function:: bing_tile_coordinates(tile) -> row<x, y>

    Returns the XY coordinates of a given Bing tile.

.. function:: bing_tile_polygon(tile) -> Geometry

    Returns the polygon representation of a given Bing tile.

.. function:: bing_tile_quadkey(tile) -> varchar

    Returns the quadkey of a given Bing tile.

.. function:: bing_tile_zoom_level(tile) -> tinyint

    Returns the zoom level of a given Bing tile.

.. function:: geometry_to_bing_tiles(geometry, zoom_level) -> array<BingTile>

    Returns the minimum set of Bing tiles that fully covers a given geometry at
    a given zoom level. Zoom levels from 1 to 23 are supported.
