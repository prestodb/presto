==============
Event Listener
==============

Presto supports custom event listeners that are invoked for the following
events:

* Query creation
* Query completion (success or failure)
* Split completion (success or failure)

This functionality enables development of custom logging, debugging and
performance analysis plugins. In a Presto cluster, only a single event
listener plugin can be active at a time .

Implementation
--------------

``EventListenerFactory`` is responsible for creating an
``EventListener`` instance. It also defines an ``EventListener``
name which is used by the administrator in a Presto configuration.
Implementations of ``EventListener`` implement methods for the event types
they are interested in handling.

The implementation of ``EventListener`` and ``EventListenerFactory``
must be wrapped as a plugin and installed on the Presto cluster.

Configuration
-------------

After a plugin that implements ``EventListener`` and
``EventListenerFactory`` has been installed on the coordinator, it is
configured using an ``etc/event-listener.properties`` file. All of the
properties other than ``event-listener.name`` are specific to the
``EventListener`` implementation.

The ``event-listener.name`` property is used by Presto to find a registered
``EventListenerFactory`` based on the name returned by
``EventListenerFactory.getName()``. The remaining properties are passed
as a map to ``EventListenerFactory.create()``.

Example configuration file:

.. code-block:: none

    event-listener.name=custom-event-listener
    custom-property1=custom-value1
    custom-property2=custom-value2
