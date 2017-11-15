==========================
Session Properties Manager
==========================

A session properties manager plugin provides a way to dynamically override default
session properties. It supports overriding default session properties for each
session based on the ``SessionConfigurationContext``, which includes ``user``,
``source``, and ``clientTags``. The returned properties override the default
values. However, session properties set by users are always respected, and take
precedence over these values.

Implementation
--------------

The ``SessionPropertyConfigurationManagerFactory`` is responsible for creating an
instance of ``SessionPropertyConfigurationManager``. Implementations of
``SessionPropertyConfigurationManager`` should provide two methods:

* ``getSystemSessionProperties``: Returns a map of system session properties.

* ``getCatalogSessionProperties``: Returns a map of catalog to catalog properties.
