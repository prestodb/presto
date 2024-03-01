=============
Release 0.277
=============

**Details**
===========

General Changes
_______________
* Fix compilation error in common subexpression elimination when duplicate expressions are present.
* Fix enforcement of query analyzer timeout during planning.
* Add UDF :func:`find_first` to find the first array element which matches a predicate.
* Add support to fail query when number of leaf plan nodes exceeds a limit. This can be controlled with ``leaf_node_limit_enabled`` and ``max_leaf_nodes_in_plan`` session properties.

Delta Lake Connector Changes
____________________________
* Upgrade Delta Standalone to 0.5.0.

Pinot Connector Changes
_______________________
* Add Pinot ``BINARY`` column type.
* Add Pinot UDF :func:`pinot_binary_decimal_to_double` to transform Pinot binary column to Double.

**Credits**
===========

Ajay George, Amit Dutta, Arjun Gupta, Arunachalam Thirupathi, Beinan, Chunxu Tang, Deepak Majeti, Feilong Liu, Ge Gao, Lin Liu, Masha Basmanova, Michael Shang, Naveen Kumar Mahadevuni, Neerad Somanchi, Nikhil Collooru, Pranjal Shankhdhar, Rebecca Schlussel, Reetika Agrawal, Rohit Jain, Sergey Pershin, Sergii Druzkin, Svetoslav Zhelev, Timothy Meehan, Todd Gao, Xinli Shang, dnskr, miomiocat, pratyakshsharma, tanjialiang
