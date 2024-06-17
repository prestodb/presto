---
id: components-and-maintainers
title: Components and Maintainers
---

## Summary

This page lists individuals of interest in the Velox community, as described in
the [Technical Governance Mechanics](./technical-governance)
page. It is meant to be used by contributors and other stakeholders as a
blueprint to find individuals responsible for different aspects of the project
and codebase.

Maintainership status is *lagging*, not *leading*, and it is only acquired
through active participation and demonstration of skills and domain-specific
knowledge. All individuals listed in this page are expected to uphold
[Velox’s mission, design philosophy, and principles](./design-philosophy). 

## Project Leadership Council - PLC

The PLC is reponsible for the long-term directional leadership of the project:

* Masha Basmanova - [mbasmanova](https://github.com/mbasmanova) / mbasmanova@meta.com - **Chair**
* Orri Erling - [oerling](https://github.com/oerling) / oerling@meta.com
* Pedro Pedreira - [pedroerp](https://github.com/pedroerp) / pedroerp@meta.com
* Xiaoxuan Meng - [xiaoxmeng](https://github.com/xiaoxmeng) / xiaoxmeng@meta.com

## Component Maintainers

Component maintainers are responsible for triaging, reviewing, and approving
pull requests concerning the component. They also maintain, enhance,
and advocate for improvements in component design, tests, CI health, and
developer documentation.

Before working on a new feature or optimization, please review our
[CONTRIBUTING.md](https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md)
guide and initiate a discussion on Github with the people listed as
maintainers of that component. 

### Vectors, Types, Arrow Bindings:

* Jimmy Lu - [Yuhta](https://github.com/Yuhta) / jimmylu@meta.com
* Masha Basmanova - [mbasmanova](https://github.com/mbasmanova) / mbasmanova@meta.com
* Pedro Pedreira - [pedroerp](https://github.com/pedroerp) / pedroerp@meta.com

### Expression Evaluation

* Masha Basmanova - [mbasmanova](https://github.com/mbasmanova) / mbasmanova@meta.com

### Operators

* Jimmy Lu - [Yuhta](https://github.com/Yuhta) / jimmylu@meta.com
* Masha Basmanova - [mbasmanova](https://github.com/mbasmanova) / mbasmanova@meta.com
* Xiaoxuan Meng - [xiaoxmeng](https://github.com/xiaoxmeng) / xiaoxmeng@meta.com

### Presto Functions

* Jimmy Lu - [Yuhta](https://github.com/Yuhta) / jimmylu@meta.com
* Masha Basmanova - [mbasmanova](https://github.com/mbasmanova) / mbasmanova@meta.com

### Spark Functions

* Rui Mo - [rui-mo](https://github.com/rui-mo) / rui.mo@intel.com

### Memory Management

* Xiaoxuan Meng - [xiaoxmeng](https://github.com/xiaoxmeng) / xiaoxmeng@meta.com

### IO, Cache, Hive connector, and File reader/writers

* Jimmy Lu - [Yuhta](https://github.com/Yuhta) / jimmylu@meta.com
* Xiaoxuan Meng - [xiaoxmeng](https://github.com/xiaoxmeng) / xiaoxmeng@meta.com

#### Parquet

* Deepak Majeti - [majetideepak](https://github.com/majetideepak) / deepak.majeti@ibm.com

#### ORC, DWRF, and Nimble

* Jimmy Lu - [Yuhta](https://github.com/Yuhta) / jimmylu@meta.com

### Storage Adapters (S3/HDFS/GCS/ABFS)

* Deepak Majeti - [majetideepak](https://github.com/majetideepak) / deepak.majeti@ibm.com

### Builds and CI

* Deepak Majeti - [majetideepak](https://github.com/majetideepak) / deepak.majeti@ibm.com
* Jacob Wujciak-Jens - [assignUser](https://github.com/assignUser) / jacob@voltrondata.com
* Krishna Pai - [kgpai](https://github.com/kgpai) / kpai@meta.com

### Fuzzers and Test Frameworks

* Masha Basmanova - [mbasmanova](https://github.com/mbasmanova) / mbasmanova@meta.com
* Pedro Pedreira - [pedroerp](https://github.com/pedroerp) / pedroerp@meta.com
