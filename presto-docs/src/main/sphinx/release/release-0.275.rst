=============
Release 0.275
=============

**Highlights**
==============

**Details**
===========

General Changes
_______________
* Fixing Disaggregated Coordinator to not wrongly identifying running queries as leaked.
* Improve the memory limit error message with allocationTag.
* Add :func:`trimmed_mean` for :ref:`tdigest <tdigest_type>`. The prototype of the function is: trimmed_mean(tdigest: TDIGEST, lower_quantile: DOUBLE, upper_quantile: DOUBLE) -> DOUBLE.
* Add a router in front of presto clusters. The router is deployed as a standalone service with a presto-styled UI.
* Add new fields in Iceberg files system table.
* Add raft server to resource manager with disaggregated coordinators using Apache Ratis. Raft server enabled in resource manager using the ``isEnabled`` property in Config file.
* Add session property ``query_max_output_positions`` and configuration property ``query.max-output-positions`` to control how many rows a query can output. The query might end up returning more rows than the limit as the check is asynchronous.

Router Changes
______________
* Add a random choice scheduler in the Presto router.
* Add a user hash scheduler in the Presto router.

**Credits**
===========

ARUNACHALAM THIRUPATHI, Ajay George, Amit Dutta, Arjun Gupta, Beinan, Chunxu Tang, Eric Kwok, Gurmeet Singh, Harleen Kaur, James Sun, JySongWithZhangCe, Karan Dadlani, Leon So, Maria Basmanova, Michael Shang, Patrick Sullivan, Paul Meng, Pranjal Shankhdhar, Reetika Agrawal, Rongrong Zhong, Ruslan Mardugalliamov, Sacha Viscaino, Sergii Druzkin, Shanyue Wan, Sreeni Viswanadha, Swapnil Tailor, Timothy Meehan, Todd Gao, Vivek, Xinli Shang, Ying, choumei, dizc, macroguo, presto-release-bot, prithvip, v-jizhang, xiaoxmeng, zhangyanbing, 付智杰
