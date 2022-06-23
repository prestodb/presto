=============
Release 0.250
=============

**Details**
===========

General Changes
_______________
* Fix a memory leak in ``NodeTaskMap`` which could lead to Full GC or OOMs in coordinator.
* Add support for configuring the maximum number of unacknowledged source splits per task. This can be enabled by setting the ``max_unacknowledged_splits_per_task`` session property or ``node-scheduler.max-unacknowledged-splits-per-task`` configuration property.
* Add bitwise shift functions :func:`bitwise_left_shift`, :func:`bitwise_right_shift`, :func:`bitwise_right_shift_arithmetic` (:pr:`15730`)
* Add support for creating, dropping, querying and seeing materialized view definitions. (:pr:`15589`, :pr:`15779`)

**Contributors**
================

Ahmad Ghazal, Ajay George, Ariel Weisberg, Arunachalam Thirupathi, David Stryker, Ge Gao, James Petty, James Sun, Lisa Yang, Maria Basmanova, Mayank Garg, Rebecca Schlussel, Rongrong Zhong, Rumeshkrishnan Mohan, Shixuan Fan, Timothy Meehan, Yang Yang, Zhenxiao Luo, tanjialiang, v-jizhang
