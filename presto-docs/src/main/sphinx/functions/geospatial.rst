=====================
GeoSpatial Functions
=====================

Presto supports OpenGIS and SQL/MM specification

Constructors
------------

.. function:: st_point(double, double) -> point

    Constructs geometry type Point object.

.. function:: st_line(varchar) -> line

    Constructs geometry type Line object.

.. function:: st_polygon(varchar) -> polygon

    Constructs geometry type Polygon object.

Relationship Tests
------------------

.. function:: st_contains(geometry, geometry) -> boolean

    Returns true if and only if left geometry contains right geometry.

.. function:: st_crosses(geometry, geometry) -> boolean

    Returns true if and only if left geometry crosses right geometry.

.. function:: st_disjoint(geometry, geometry) -> boolean

    Returns true if and only if the intersection of left geometry and right geometry is empty.

.. function:: st_envelope_intersect(geometry, geometry) -> boolean

    Returns true if and only if the envelopes of left geometry and right geometry intersect.

.. function:: st_equals(geometry, geometry) -> boolean

    Returns true if and only if left geometry equals right geometry.

.. function:: st_intersects(geometry, geometry) -> boolean

    Returns true if and only if left geometry intersects right geometry.

.. function:: st_overlaps(geometry, geometry) -> boolean

    Returns true if and only if left geometry overlaps right geometry.

.. function:: st_relate(geometry, geometry) -> boolean

    Returns true if and only if left geometry has the specified DE-9IM relationship with right geometry.

.. function:: st_touches(geometry, geometry) -> boolean

    Returns true if and only if left geometry touches right geometry.

.. function:: st_within(geometry, geometry) -> boolean

    Returns true if and only if left geometry is within right geometry.

Operations
----------

.. function:: st_boundary(geometry) -> geometry

    Returns binary representation of the boundary geometry of input geometry.

.. function:: st_buffer(geometry, double) -> geometry

    Returns binary representation of the geometry buffered by distance.

.. function:: st_difference(geometry, geometry) -> geometry

    Returns binary representation of the geometry difference of left geometry and right geometry.

.. function:: st_envelope(geometry) -> geometry

    Returns binary representation of envelope of the input geometry.

.. function:: st_exterior_ring(geometry) -> geometry

    Returns binary representation of the exterior ring of the polygon.

.. function:: st_intersection(geometry, geometry) -> geometry

    Returns binary representation of the geometry intersection of left geometry and right geometry.

.. function:: st_symmetric_difference(geometry, geometry) -> geometry

    Returns binary representation of the geometry symmetric difference of left geometry and right geometry.

Accessors
---------

.. function:: st_area(geometry) -> double

    Returns area of input polygon.

.. function:: st_centroid(geometry) -> varchar

    Returns point that is the center of the polygon's envelope.

.. function:: st_coordinate_dimension(geometry) -> bigint

    Returns count of coordinate components.

.. function:: st_dimension(geometry) -> bigint

    Returns spatial dimension of geometry.

.. function:: st_distance(geometry, geometry) -> double

    Returns distance between left geometry and right geometry.

.. function:: st_is_closed(geometry) -> boolean

    Returns true if and only if the line is closed.

.. function:: st_is_empty(geometry) -> boolean

    Returns true if and only if the geometry is empty.

.. function:: st_is_ring(geometry) -> boolean

    Returns true if and only if the line is closed and simple.

.. function:: st_length(geometry) -> double

    Returns the length of line.

.. function:: st_max_x(geometry) -> double

    Returns the maximum X coordinate of geometry.

.. function:: st_max_y(geometry) -> double

    Returns the maximum Y coordinate of geometry.

.. function:: st_min_x(geometry) -> double

    Returns the minimum X coordinate of geometry.

.. function:: st_min_y(geometry) -> double

    Returns the minimum Y coordinate of geometry.

.. function:: st_start_point(geometry) -> point

    Returns the first point of an line.

.. function:: st_end_point(geometry) -> point

    Returns the last point of an line.

.. function:: st_x(point) -> double

    Returns the X coordinate of point.

.. function:: st_y(point) -> double

    Returns the Y coordinate of point.

.. function:: st_point_number(geometry) -> bigint

    Returns the number of points in the geometry.

.. function:: st_interior_ring_number(geometry) -> bigint

    Returns the number of interior rings in the polygon.

GeoSpatial Join Optimization
----------------------------

GeoSpatial join is supported in Presto as cross join. Here is an example, we have events table, which has
event location longitude and latitude. We also have cities table, which has city id and city shape.
To group events happenning in each city::

    SELECT city_id, count(*)
    FROM event_table
    CROSS JOIN city_table
    ON st_contains(city_shape, st_point(event.location.lng, event.location.lat))
    GROUP BY 1

In GeoSpatial Join Optimization, we could use build_geo_index to build a QuadTree using city_id and city_shape,
Then geo_contains could use this QuadTree to find out city_id containing the event location::

    SELECT city_id, count(*)
    FROM event_table
    CROSS JOIN (
        SELECT build_geo_index(city_id, city_shape) as geo_index
        FROM city_table
    )
    WHERE geo_contains(st_point(event.location.lng, event.location.lat), geo_index) is not null
    GROUP BY 1
