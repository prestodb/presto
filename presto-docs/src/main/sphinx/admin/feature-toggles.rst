===============
Feature Toggles
===============

The default Presto settings should work well for most workloads. The following
information may help you if some of the features are not working correctly for you.
Though, if you think the behavior of a feature is unexpected, don't hesitate to file a bug report regardless if it's enabled or disabled by default.

Config Properties
-----------------

+--------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|Name of property          | Default value | Description                                                                                                                                                                                                                                             |
+==========================+===============+=========================================================================================================================================================================================================================================================+
|optimizer.reorder-windows | true          |Allow reordering windows in order to put those with the same partitioning next to each other. This will sometimes decrease the number of repartitionings. This can also be specified on a per-query basis using the ``reorder_windows`` session property.|
+--------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
