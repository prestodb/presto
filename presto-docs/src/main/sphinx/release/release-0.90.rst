============
Release 0.90
============


General Changes
---------------
* Add ConnectorPageSink which is a more efficient interface for column-oriented sources.
* Add property ``task.writer-count`` to configure the number of writers per task.

SPI Changes
-----------
* Add ``getColumnTypes`` to ``RecordSink``

.. note::
    This is a backwards incompatible change with the previous connector SPI.
    If you have an implementation of ``RecordSink`` you need to update
    your code to add the ``getColumnTypes`` method.
