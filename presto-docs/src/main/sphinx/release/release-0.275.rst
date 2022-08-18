=============
Release 0.275
=============

**Details**
===========

General Changes
_______________
* Fix Disaggregated Coordinator to correctly identify running queries as leaked.
* Add :func:`trimmed_mean` for :ref:`tdigest <tdigest_type>`.
* Add :func:`construct_tdigest()` for :ref:`tdigest <tdigest_type>`.
* Add session property ``query_max_output_positions`` and configuration property ``query.max-output-positions`` to control how many rows a query can output. The query might end up returning more rows than the limit as the check is asynchronous.

Hudi Changes
______________
* Add support for reading log-only on hudi MOR table.

Router Changes
______________
* Add a router to route queries to presto clusters. The new router supports random choice scheduling and user hash scheduling. The router is deployed as a standalone service with a presto-styled UI.

Spark Changes
______________
* Add support for setting session properties for presto on spark.

**Credits**
===========

ARUNACHALAM THIRUPATHI, Ajay George, Amit Dutta, Arjun Gupta, Beinan, Chunxu Tang, Eric Kwok, Gurmeet Singh, Harleen Kaur, James Sun, JySongWithZhangCe, Karan Dadlani, Leon So, Maria Basmanova, Michael Shang, Patrick Sullivan, Paul Meng, Pranjal Shankhdhar, Reetika Agrawal, Rongrong Zhong, Ruslan Mardugalliamov, Sacha Viscaino, Sergii Druzkin, Shanyue Wan, Sreeni Viswanadha, Swapnil Tailor, Timothy Meehan, Todd Gao, Vivek, Xinli Shang, Ying, choumei, dizc, macroguo, presto-release-bot, prithvip, v-jizhang, xiaoxmeng, zhangyanbing, 付智杰
