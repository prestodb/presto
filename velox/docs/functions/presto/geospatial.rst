====================
Geospatial Operators
====================

Geometry Functions
------------------

Velox has a set of functions that operator on Geometries, conforming to the Open
Geospatial Consortium's (OGC) `OpenGIS Specifications`_. These are
piecewise-linear planar geometries that are 0-, 1-, or 2-dimensional and
collections of such geometries. They support a variety of operations, including
spatial relationship predicates (e.g., "do these geometries intersect?"), binary
operations (e.g., "what is the union of these geometries?"), and unary
operations (e.g., "what is the area of this geometry?"). The basis for the
Geometry type is a plane. The shortest path between two points on the plane is a
straight line. That means calculations on geometries (areas, distances, lengths,
intersections, etc) can be calculated using cartesian mathematics and straight
line vectors.

Functions that begin with the ST_ prefix are compliant with the `SQL/MM Part 3:
Spatial`_ specification`. As such, many Geometry functions assume that
geometries that are operated on are both simple and valid. Functions may raise
an exception or return an arbitrary value for non-simple or non-valid
geometries. For example, it does not make sense to calculate the area of a
polygon that has a hole defined outside of the polygon, or to construct a
polygon from a non-simple boundary line.

Geometries can be constructed in several ways. Two of the most common are
``ST_GeometryFromText`` and ``ST_GeomFromBinary``.  ``ST_GeometryFromText``
accepts the Well-Known Text (WKT) format::

* POINT (0 0)
* LINESTRING (0 0, 1 1, 1 2)
* POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))
* MULTIPOINT (0 0, 1 2)
* MULTILINESTRING ((0 0, 1 1, 1 2), (2 3, 3 2, 5 4))
* MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)),
  ((-1 -1, -1 -2, -2 -2, -2 -1, -1 -1)))
* GEOMETRYCOLLECTION (POINT(2 3), LINESTRING (2 3, 3 4))

``ST_GeomFromBinary`` accepts the Well-Known Binary (WKB) format. In WKT/WKB,
the coordinate order is (x, y). The details of both WKT and WKB can be found
`here
<https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry>`_.

Geometry Constructors
---------------------

.. function:: ST_GeometryFromText(wkt: varchar) -> geometry: Geometry

    Creates a Geometry object from the Well-Known Text (WKT) formatted string.
    Ill-formed strings will return a User Error.

.. function:: ST_GeomFromBinary(wkb: varbinary) -> geometry: Geometry

    Creates a Geometry object from the Well-Known Binary (WKB) formatted binary.
    Ill-formed binary will return a User Error.

.. function:: ST_AsText(geometry: Geometry) -> wkt: varchar

    Returns the Well-Known Text (WKT) formatted string of the Geometry object.

.. function:: ST_AsBinary(geometry: Geometry) -> wkb: varbinary

    Returns the Well-Known Binary (WKB) formatted binary of the Geometry object.

.. function:: ST_Point(x: double, y: double) -> geometry: Geometry

    Returns the Point geometry at the given coordinates.  This will raise an
    error if ``x`` or ``y`` is ``NaN`` or ``infinity``.

.. function:: ST_Polygon(wkt: varchar) -> polygon: Geometry

    Returns a geometry type polygon object from WKT representation.

Spatial Predicates
------------------

Spatial Predicates are binary functions that return a boolean value indicating
whether a given spatial relationship holds. These relations are defined by the
`DE-9IM`_ intersection matrix model. The model defines intersection patterns such
as "overlaps" or "touches." The exact conditions can be surprising (especially
for empty geometries), so be sure to read the documentation for the specific
function you are using.

.. function:: ST_Contains(geometry1: Geometry, geometry2: Geometry) -> boolean

    Returns ``true`` if and only if no points of the second geometry lie in the
    exterior of the first geometry, and at least one point of the interior of
    the first geometry lies in the interior of the second geometry.

.. function:: ST_Crosses(geometry1: Geometry, geometry2: Geometry) -> boolean

    Returns ``true`` if the supplied geometries have some, but not all, interior
    points in common.

.. function:: ST_Disjoint(geometry1: Geometry, geometry2: Geometry) -> boolean

    Returns ``true`` if the give geometries do not spatially intersect -- if
    they do not share any space together.

.. function:: ST_Equals(geometry1: Geometry, geometry2: Geometry) -> boolean

    Returns ``true`` if the given geometries represent the same geometry.

.. function:: ST_Intersects(geometry1: Geometry, geometry2: Geometry) -> boolean

    Returns ``true`` if the given geometries spatially intersect in two
    dimensions (share any portion of space) and ``false``0 if they do not (they
    are disjoint).

.. function:: ST_Overlaps(geometry1: Geometry, geometry2: Geometry) -> boolean

    Returns ``true`` if the given geometries share space, are of the same
    dimension, but are not completely contained by each other.

.. function:: ST_Relat(geometry1: Geometry, geometry2: Geometry, relation: varchar) -> boolean

    Returns true if first geometry is spatially related to second geometry as
    described by the relation.  The relation is a string like ``'"1*T***T**'``:
    please see the description of DM-9IM for more details.

.. function:: ST_Touches(geometry1: Geometry, geometry2: Geometry) -> boolean

    Returns ``true`` if the given geometries have at least one point in common,
    but their interiors do not intersect.

.. function:: ST_Within(geometry1: Geometry, geometry2: Geometry) -> boolean

    Returns true if first geometry is completely inside second geometry.


.. _DE-9IM: https://en.wikipedia.org/wiki/DE-9IM

Spatial Operations
------------------

.. function:: ST_Boundary(geometry: Geometry) -> boundary: Geometry

    Returns the closure of the combinatorial boundary of ``geometry``.
     Empty geometry inputs result in empty output.

.. function:: ST_Difference(geometry1: Geometry, geometry2: Geometry) -> difference: Geometry

    Returns the geometry that represents the portion of ``geometry1`` that is
    not contained in ``geometry2``.

.. function:: ST_Intersection(geometry1: Geometry, geometry2: Geometry) -> intersection: Geometry

    Returns the geometry that represents the portion of ``geometry1`` that is
    also contained in ``geometry2``.

.. function:: ST_SymDifference(geometry1: Geometry, geometry2: Geometry) -> symdifference: Geometry

    Returns the geometry that represents the portion of ``geometry1`` that is
    not contained in ``geometry2`` as well as the portion of ``geometry1`` that
    is not congtained in ``geometry1``.

.. function:: ST_Union(geometry1: Geometry, geometry2: Geometry) -> intersection: Geometry

    Returns the geometry that represents the all points in either ``geometry1``
    or ``geometry2``.

.. function:: ST_Envelope(geometry: Geometry) -> envelope: Geometry

    Returns the bounding rectangular polygon of a ``geometry``. Empty input will
    result in empty output.

.. function:: ST_ExteriorRing(geometry: Geometry) -> output: Geometry

    Returns a LineString representing the exterior ring of the input polygon.
    Empty or null inputs result in null output. Non-polygon types will return
    an error.

.. function:: expand_envelope(geometry: Geometry, distance: double) -> output: Geometry

    Returns the bounding rectangular polygon of a geometry, expanded by a distance.
    Empty geometries will return an empty polygon. Negative or NaN distances will
    return an error. Positive infinity distances may lead to undefined results.


Accessors
---------
.. function:: ST_IsValid(geometry: Geometry) -> valid: bool

    Returns if ``geometry`` is valid, according to `SQL/MM Part 3: Spatial`_.
    Examples of non-valid geometries include Polygons with self-intersecting shells.

.. function:: ST_IsSimple(geometry: Geometry) -> simple: bool

    Returns if ``geometry`` is simple, according to `SQL/MM Part 3: Spatial`_.
    Examples of non-simple geometries include LineStrings with self-intersections,
    Polygons with empty rings for holes, and more.

.. function:: ST_IsClosed(geometry: Geometry) -> closed: bool

    Returns true if the LineString’s start and end points are coincident. Will
    return an error if the input geometry is not a LineString or MultiLineString.

.. function:: ST_IsRing(geometry: Geometry) -> ring: bool

   Returns true if and only if the line is closed and simple. Will return an error
   if input geometry is not a LineString.

.. function:: ST_IsEmpty(geometry: Geometry) -> empty: bool

   Returns true if and only if this Geometry is an empty GeometryCollection, Polygon,
   Point etc.

.. function:: ST_Length(geometry: Geometry) -> length: double

   Returns the length of a LineString or MultiLineString using Euclidean measurement
   on a two dimensional plane (based on spatial ref) in projected units. Will
   return an error if the input geometry is not a LineString or MultiLineString.

.. function:: ST_PointN(linestring: Geometry, index: integer) -> point: geometry

   Returns the vertex of a LineString at a given index (indices start at 1).
   If the given index is less than 1 or greater than the total number of elements
   in the collection, returns NULL.

.. function:: ST_Points(geometry: Geometry) -> points: array(geometry)

   Returns an array of points in a geometry. Empty or null inputs
   return null.

.. function:: ST_NumPoints(geometry: Geometry) -> points: integer

   Returns the number of points in a geometry. This is an extension
   to the SQL/MM ``ST_NumPoints`` function which only applies to
   point and linestring.

.. function:: geometry_nearest_points(geometry1: Geometry, geometry2: Geometry) -> points: array(geometry)

   Returns the points on each geometry nearest the other. If either geometry
   is empty, return null. Otherwise, return an array of two Points that have
   the minimum distance of any two points on the geometries. The first Point
   will be from the first Geometry argument, the second from the second Geometry
   argument. If there are multiple pairs with the minimum distance, one pair
   is chosen arbitrarily.

.. function:: ST_EnvelopeAsPts(geometry: Geometry) -> points: array(geometry)

   Returns an array of two points: the lower left and upper right corners
   of the bounding rectangular polygon of a geometry. Empty or null inputs
   return null.

.. function:: geometry_invalid_reason(geometry: Geometry) -> reason: varchar

    If ``geometry`` is not valid or not simple, return a description of the
    reason. If the geometry is valid and simple (or ``NULL``), return ``NULL``.
    This function is relatively expensive.

.. function:: ST_Area(geometry: Geometry) -> area: double

    Returns the 2D Euclidean area of ``geometry``.
    For Point and LineString types, returns 0.0. For GeometryCollection types,
    returns the sum of the areas of the individual geometries. Empty geometries
    return 0.

.. function:: ST_Centroid(geometry: Geometry) -> geometry: Geometry

    Returns the point value that is the mathematical centroid of ``geometry``.
    Empty geometry inputs result in empty output.

.. function:: ST_Distance(geometry1: Geometry, geometry2: Geometry) -> distance: double

    Returns the 2-dimensional cartesian minimum distance (based on spatial ref)
    between two geometries in projected units. Empty geometries result in null output.

.. function:: ST_GeometryType(geometry: Geometry) -> type: varchar

    Returns the type of the geometry.

.. function:: ST_X(geometry: Geometry) -> x: double

    Returns the ``x`` coordinate of the geometry if it is a Point.  Returns
    ``null`` if the geometry is empty.  Raises an error if the geometry is
    not a Point and not empty.

.. function:: ST_Y(geometry: Geometry) -> x: double

    Returns the ``y`` coordinate of the geometry if it is a Point.  Returns
    ``null`` if the geometry is empty.  Raises an error if the geometry is
    not a Point and not empty.

.. function:: ST_XMin(geometry: Geometry) -> x: double

    Returns the minimum ``x`` coordinate of the geometries bounding box.
    Returns ``null`` if the geometry is empty.

.. function:: ST_YMin(geometry: Geometry) -> y: double

    Returns the minimum ``y`` coordinate of the geometries bounding box.
    Returns ``null`` if the geometry is empty.

.. function:: ST_XMax(geometry: Geometry) -> x: double

    Returns the maximum ``x`` coordinate of the geometries bounding box.
    Returns ``null`` if the geometry is empty.

.. function:: ST_YMax(geometry: Geometry) -> y: double

    Returns the maximum ``y`` coordinate of the geometries bounding box.
    Returns ``null`` if the geometry is empty.

.. function:: ST_StartPoint(geometry: Geometry) -> point: Geometry

    Returns the first point of a LineString geometry as a Point.
    This is a shortcut for ``ST_PointN(geometry, 1)``. Empty
    input will return ``null``.

.. function:: ST_EndPoint(geometry: Geometry) -> point: Geometry

    Returns the last point of a LineString geometry as a Point.
    This is a shortcut for ``ST_PointN(geometry, ST_NumPoints(geometry))``.
    Empty input will return ``null``.

.. function:: ST_GeometryN(geometry: Geometry, index: integer) -> geometry: Geometry

    Returns the ``geometry`` element at a given index (indices start at 1).
    If the ``geometry`` is a collection of geometries (e.g., GeometryCollection or
    Multi*), returns the ``geometry`` at a given index. If the given index is less
    than 1 or greater than the total number of elements in the collection, returns
    NULL. Use ``:func:ST_NumGeometries`` to find out the total number of elements.
    Singular geometries (e.g., Point, LineString, Polygon), are treated as
    collections of one element. Empty geometries are treated as empty collections.

.. function:: ST_InteriorRingN(geometry: Geometry, index: integer) -> geometry: Geometry

    Returns the interior ring element at the specified index (indices start at 1).
    If the given index is less than 1 or greater than the total number of interior
    rings in the input ``geometry``, returns NULL. Throws an error if the input geometry
    is not a polygon. Use ``:func:ST_NumInteriorRing`` to find out the total number of
    elements.

.. function:: ST_NumGeometries(geometry: Geometry) -> output: integer

    Returns the number of geometries in the collection. If the geometry is a
    collection of geometries (e.g., GeometryCollection or Multi*),
    returns the number of geometries, for single geometries returns 1,
    for empty geometries returns 0. Note that empty geometries inside of a
    GeometryCollection will count as a geometry if and only if there is at
    least 1 non-empty geometry in the collection. e.g.
    ``ST_NumGeometries(ST_GeometryFromText('GEOMETRYCOLLECTION(POINT EMPTY)'))``
    will evaluate to 0, but
    ``ST_NumGeometries(ST_GeometryFromText('GEOMETRYCOLLECTION(POINT EMPTY, POINT (1 2))'))``
    will evaluate to 1.

.. function:: ST_InteriorRings(geometry: Geometry) -> output: array(geometry)

    Returns an array of all interior rings found in the input geometry,
    or an empty array if the polygon has no interior rings. Returns
    null if the input geometry is empty.
    Throws an error if the input geometry is not a polygon.

.. function:: ST_Geometries(geometry: Geometry) -> output: array(geometry)

    Returns an array of geometries in the specified collection. Returns
    a one-element array if the input geometry is not a multi-geometry.
    Returns null if input geometry is empty. For example, a MultiLineString
    will create an array of LineStrings. A GeometryCollection will
    produce an un-flattened array of its constituents:
    GEOMETRYCOLLECTION (MULTIPOINT(0 0, 1 1),
    GEOMETRYCOLLECTION (MULTILINESTRING((2 2, 3 3))) ) would produce
    array[MULTIPOINT(0 0, 1 1), GEOMETRYCOLLECTION( MULTILINESTRING((2 2, 3 3)) )]

.. function:: flatten_geometry_collections(geometry: Geometry) -> output: array(geometry)

    Recursively flattens any GeometryCollections in Geometry, returning an array
    of constituent non-GeometryCollection geometries. The order of the array
    is arbitrary and should not be relied upon. null input results in null output.
    Examples:

    POINT (0 0) -> [POINT (0 0)], MULTIPOINT (0 0, 1 1) -> [MULTIPOINT (0 0, 1 1)],
    GEOMETRYCOLLECTION (POINT (0 0), GEOMETRYCOLLECTION (POINT (1 1))) ->
    [POINT (0 0), POINT (1 1)], GEOMETRYCOLLECTION EMPTY -> [].

.. function:: ST_NumInteriorRing(geometry: Geometry) -> output: integer

    Returns the cardinality of the collection of interior rings of a polygon.

.. function:: ST_ConvexHull(geometry: Geometry) -> output: Geometry

    Returns the minimum convex geometry that encloses all input geometries.

.. function:: ST_CoordDim(geometry: Geometry) -> output: integer

    Return the coordinate dimension of the geometry.

.. function:: ST_Dimension(geometry: Geometry) -> output: tinyint

    Returns the inherent dimension of this geometry object, which
    must be less than or equal to the coordinate dimension.

.. function:: ST_ExteriorRing(geometry: Geometry) -> output: Geometry

    Returns a line string representing the exterior ring of the input polygon.

.. function:: ST_Buffer(geometry: Geometry, distance: double) -> output: Geometry

    Returns the geometry that represents all points whose distance from the
    specified ``geometry`` is less than or equal to the specified ``distance``.
    If the points of the ``geometry`` are extremely close together
    (delta < 1e-8), this might return an empty geometry. Empty inputs return
    null.

.. function:: simplify_geometry(geometry: Geometry, tolerance: double) -> output: Geometry

    Returns a "simplified" version of the input geometry using the
    Douglas-Peucker algorithm. Will avoid creating geometries (polygons in
    particular) that are invalid. Tolerance must be a non-negative finite value.
    Using tolerance of 0 will return the original geometry.  Empty geometries
    will also be returned as-is.

.. function:: line_locate_point(linestring: Geometry, point: Geometry) -> output: double

    Returns a float between 0 and 1 representing the location of the closest
    point on the LineString to the given Point, as a fraction of total 2d line length.

    Returns null if a LineString or a Point is empty or null.

.. function:: line_interpolate_point(linestring: Geometry, fraction: double) -> output: geometry

    Returns the Point on the LineString at a fractional distance given by
    the double argument. Throws an exception if the distance is not between 0 and 1.

    Returns an empty Point if the LineString is empty.
    Returns null if either the LineString or double is null.

Bing Tile Functions
-------------------

Bing tiles are a convenient quad-tree representation of the WGS84 projection of
Earth's surface. They can be used to partition geospatial data, perform quick
proximity or intersection checks, and more. Each tile is defined by a `zoom`
level (how far down the quad-tree the tile lives), and an `x` and `y` coordinate
specifying where it is in that `zoom` level.  Velox supports `zoom` levels from
0 to 23.  For a given zoom level, `x` and `y` must be between 0 and `2**zoom -
1` inclusive.  Lower `x` values are west of higher `x` values, and lower `y`
values are north of higher `y` values.

Bing tiles can be cast to and from an efficient BIGINT representation. While every
Bing tile can be cast to a valid BIGINT, not every BIGINT is a valid Bing tile, so
casting BIGINT to BINGTILE may fail.

::
    CAST(CAST(tile AS BIGINT) AS BINGTILE)

See https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system
for more details.

.. function:: bing_tile(x: integer, y: integer, zoom_level: tinyint) -> tile: BingTile

    Creates a Bing tile object from ``x``, ``y`` coordinates and a ``zoom_level``.
    Zoom levels from 0 to 23 are supported, with valid ``x`` and ``y`` coordinates
    described above.  Invalid parameters will return a User Error.

.. function:: bing_tile(quadKey: varchar) -> tile: BingTile

    Creates a Bing tile object from a quadkey. An invalid quadkey will return a User Error.

.. function:: bing_tile_coordinates(tile: BingTile) -> coords: row(integer,integer)

    Returns the ``x``, ``y`` coordinates of a given Bing tile as ``row(x, y)``.

.. function:: bing_tile_zoom_level(tile: BingTile) -> zoom_level: tinyint

    Returns the zoom level of a given Bing tile.

.. function:: bing_tile_parent(tile) -> parent: BingTile

    Returns the parent of the Bing tile at one lower zoom level. Throws an
    exception if tile is at zoom level 0.

.. function:: bing_tile_parent(tile, parentZoom) -> parent: BingTile

    Returns the parent of the Bing tile at the specified lower zoom level.
    Throws an exception if parentZoom is less than 0, or parentZoom is greater
    than the tile's zoom.

.. function:: bing_tile_children(tile) -> children: array(BingTile)

    Returns the children of the Bing tile at one higher zoom level. Throws an
    exception if tile is at max zoom level.

.. function:: bing_tile_children(tile, childZoom) -> children: array(BingTile)

    Returns the children of the Bing tile at the specified higher zoom level.
    Throws an exception if childZoom is greater than the max zoom level, or
    childZoom is less than the tile's zoom.  The order is deterministic but not
    specified.

.. function:: bing_tile_quadkey() -> quadKey: varchar

    Returns the quadkey representing the provided bing tile.


.. _OpenGIS Specifications: https://www.ogc.org/standards/ogcapi-features/
.. _SQL/MM Part 3: Spatial: https://www.iso.org/standard/31369.html
