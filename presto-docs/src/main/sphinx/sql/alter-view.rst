==========
ALTER VIEW
==========

Synopsis
--------

.. code-block:: sql

    ALTER VIEW [IF EXISTS] old_view_name RENAME TO new_view_name;

Description
-----------

The ``ALTER VIEW [IF EXISTS] RENAME TO`` statement renames an existing view to a
new name. This allows you to change the name of a view without having to drop
and recreate it. The view's definition, security settings, and dependencies
remain unchanged; only the name of the view is updated.

The optional ``IF EXISTS`` clause prevents an error from being raised if the
view does not exist. Instead, no action is taken, and a notice is issued.

Renaming a view does not affect the data or structure of the underlying
query used to define the view. Any permissions or dependencies on the
view are retained, and queries or applications using the old name must
be updated to use the new name.

Examples
--------

Rename the view ``users`` to ``people``::

    ALTER VIEW users RENAME TO people;

Rename the view ``users`` to ``people`` if view ``users`` exists::

    ALTER VIEW IF EXISTS users RENAME TO people;

See Also
--------

:doc:`create-view`
