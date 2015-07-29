=============
Release 0.119
=============

General Changes
---------------

* Add :func:`geometric_mean` function.
* Fix restoring interrupt status in ``StatementClient``.
* Fix bug in ORC reader where ``VARCHAR`` or ``VARBINARY`` value may be
  incorrectly read as null.
* Execute views with the permissions of the view owner.
* Add owner to view metadata.

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have written a connector that supports views, you will need to
    update your code to the new APIs.


CLI Changes
-----------

* Fix handling of full width characters.
* Skip printing query URL if terminal is too narrow.
* Allow performing a partial query cancel using ``ctrl-P``.
* Allow toggling debug mode during query by pressing ``D``.
