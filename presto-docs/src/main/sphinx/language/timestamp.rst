========================
Legacy And New Timestamp
========================

New ``TIMESTAMP`` and ``TIME`` semantics align the types with the SQL standard.
See the following sections for details.

.. note::

   The new ``TIMESTAMP`` semantics is still experimental. It's recommended to keep
   the legacy ``TIMESTAMP`` semantics enabled. You can experiment with the new semantics
   by configuring it globally or on a per-session basis. The legacy semantics
   may be deprecated in a future release.

Configuration
-------------

The legacy semantics can be enabled using the ``deprecated.legacy-timestamp``
config property. Setting it to ``true`` (the default) enables the legacy semantics,
whereas setting it to ``false`` enables the new semantics.

Additionally, it can be enabled or disabled on a per-session basis
with the ``legacy_timestamp`` session property.

TIMESTAMP semantic changes
~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously, the ``TIMESTAMP`` type described an instance in time in the Presto session's time zone.
Now, Presto treats ``TIMESTAMP`` values as a set of the following fields representing wall time:

 * ``YEAR OF ERA``
 * ``MONTH OF YEAR``
 * ``DAY OF MONTH``
 * ``HOUR OF DAY``
 * ``MINUTE OF HOUR``
 * ``SECOND OF MINUTE`` - as ``DECIMAL(5, 3)``

For that reason, a ``TIMESTAMP`` value is not linked with the session time zone in any way until
a time zone is needed explicitly, such as when casting to a ``TIMESTAMP WITH TIME ZONE`` or
``TIME WITH TIME ZONE``. In those cases, the time zone offset of the session time zone is applied,
as specified in the SQL standard.

TIME semantic changes
~~~~~~~~~~~~~~~~~~~~~

The ``TIME`` type was changed similarly to the ``TIMESTAMP`` type.

TIME WITH TIME ZONE semantic changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Due to compatibility requirements, having ``TIME WITH TIME ZONE`` completely aligned with the SQL
standard was not possible yet. For that reason, when calculating the time zone offset for ``TIME WITH
TIME ZONE``, Presto uses the session's start date and time.

This can be seen in queries using ``TIME WITH TIME ZONE`` in a time zone that has had time zone policy
changes or uses DST. For example, with a session start time of 2017-03-01:

 * Query: ``SELECT TIME '10:00:00 Asia/Kathmandu' AT TIME ZONE 'UTC'``
 * Legacy result: ``04:30:00.000 UTC``
 * New result: ``04:15:00.000 UTC``
