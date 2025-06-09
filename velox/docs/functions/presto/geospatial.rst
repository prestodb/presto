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

Accessors
---------
.. function:: ST_IsValid(geometry: Geometry) -> valid: bool

    Returns if ``geometry`` is valid, according to `SQL/MM Part 3: Spatial`_.
    Examples of non-valid geometries include Polygons with self-intersecting shells.

.. function:: ST_IsSimple(geometry: Geometry) -> simple: bool

    Returns if ``geometry`` is simple, according to `SQL/MM Part 3: Spatial`_.
    Examples of non-simple geometries include LineStrings with self-intersections,
    Polygons with empty rings for holes, and more.

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

.. function:: simplify_geometry(geometry: Geometry, tolerance: double) -> output: Geometry

    Returns a "simplified" version of the input geometry using the
    Douglas-Peucker algorithm. Will avoid creating geometries (polygons in
    particular) that are invalid. Tolerance must be a non-negative finite value.
    Using tolerance of 0 will return the original geometry.  Empty geometries
    will also be returned as-is.

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
