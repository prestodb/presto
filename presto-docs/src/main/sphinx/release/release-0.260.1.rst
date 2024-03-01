===============
Release 0.260.1
===============

General Changes
_______________
* Fix query failures caused due to a concurrency issue in the runtime metrics framework when Alluxio caching is enabled. (:pr:`16664`)
* Fix query failures due to ``AGG_FUNCTION(IF())`` to ``FILTER`` rewrite rule by disabling the rule by default. (:pr:`16661`)
