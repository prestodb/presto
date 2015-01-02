============
Release 0.90
============

General Changes
---------------
* Add ConnectorPageSink which is a more efficient interface for column-oriented sources.
* Add property ``task.writer-count`` to configure the number of writers per task.
* Added :doc:`/sql/set-session`, :doc:`/sql/reset-session` and :doc:`/sql/show-session`
* Replace ``min(boolean)`` with :func:`bool_and`.
* Replace ``max(boolean)`` with :func:`bool_or`.
* Add standard SQL function :func:`every` as an alias for :func:`bool_and`.

SPI Changes
-----------
* Add ``getColumnTypes`` to ``RecordSink``

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have an implementation of ``RecordSink`` you need to update
    your code to add the ``getColumnTypes`` method.
