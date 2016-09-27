=============
Release 0.153
=============

General Changes
---------------

* Fix incorrect results for grouping sets when ``task.concurrency`` is greater than one.
* Make resource group configuration more flexible. See "SPI Changes" below, and the
  :doc:`resource groups documentation </admin/resource-groups>`.
* Improve performance of :ref:`array_type` when underlying data is dictionary encoded.
* Improve performance of ORC reader when decoding dictionary encoded :ref:`map_type`.

SPI Changes
-----------

* Add support for pluggable resource group management. A ``Plugin`` can now
  provide management factories via ``getResourceGroupConfigurationManagerFactories()``
  and the factory can be enabled via the ``etc/resource-groups.properties``
  configuration file by setting the ``resource-groups.configuration-manager``
  property. See the ``presto-resource-group-managers`` plugin for an example.
