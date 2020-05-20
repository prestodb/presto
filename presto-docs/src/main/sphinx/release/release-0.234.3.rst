===============
Release 0.234.3
===============

General Changes
_______________
* Fix an issue where queries may potentially queue indefinitely.
* Fix a performance regression in JDK versions 13 and below by disabling memory allocation tracking by default.