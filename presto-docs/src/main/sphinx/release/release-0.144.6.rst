===============
Release 0.144.6
===============

General Changes
---------------

This release fixes several problems with large and negative intervals.

* Fix parsing of negative interval literals. Previously, the sign of each field was treated
  independently instead of applying to the entire interval value. For example, the literal
  ``INTERVAL '-2-3' YEAR TO MONTH`` was interpreted as a negative interval of ``21`` months
  rather than ``27`` months (positive ``3`` months was added to negative ``24`` months).
* Fix handling of ``INTERVAL DAY TO SECOND`` type in REST API. Previously, intervals greater than
  ``2,147,483,647`` milliseconds (about ``24`` days) were returned as the wrong value.
* Fix handling of ``INTERVAL YEAR TO MONTH`` type. Previously, intervals greater than
  ``2,147,483,647`` months were returned as the wrong value from the REST API
  and parsed incorrectly when specified as a literal.
* Fix formatting of negative intervals in REST API. Previously, negative intervals
  had a negative sign before each component and could not be parsed.
* Fix formatting of negative intervals in JDBC ``PrestoInterval`` classes.

.. note::

    Older versions of the JDBC driver will misinterpret most negative
    intervals from new servers. Make sure to update the JDBC driver
    along with the server.
