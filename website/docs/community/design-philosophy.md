---
id: design-philosophy
title: Velox Design Philosophy
---

## Summary

This page lists a set of directional principles and values meant to guide
contributors and maintainers as they develop the Velox project. These are not
meant to be hard-and-fast rules, but to inform decision making and help guide
discussions that may come up during the development of Velox. 

## Velox Mission

The Velox project aspires to:

> “Accelerate data management systems by providing a **reliable** and
> **unified** **state-of-the-art** **open-source** execution platform”

The *Velox community* is composed of a set of highly-skilled individuals with the
shared purpose of achieving this mission through the development and
maintenance of the **Velox open-source library** and its ecosystem. In order to
support and inform community efforts as we progress towards achieving the
mission, a few overarching principles and values are highlighted below.

## Velox Principles

* **Reliable.** To become the true execution underpinnings for data management
  systems across the industry, Velox strives for excellence in reliability.
  This means that as a community we heavily invest in enhancing our test
  infrastructure, and building sophisticated database testing and validation
  frameworks. During Velox development, we do not compromise on stability,
  reliability, or correctness.

* **Unified.** We envision that by unifying execution across data management
  systems (hence reducing the existing fragmentation), we can be more efficient
  as an engineering community. Therefore, Velox aspires to provide components
  which are engine- and dialect-agnostic, containing extensibility APIs through
  which the behavior of the library can be extended to match a desired
  semantic.

* **State-of-the-art.** As a unified platform, we believe in the opportunity of
  centralizing advanced optimizations and techniques previously only available
  in individual monolithic systems (many of which proprietary), making them
  easily available everywhere. Although there is a large set of optimizations
  that can be added (each adding a layer of complexity), we prioritize adding
  the features that provide the largest performance improvement to real
  workloads - as opposed to synthetic benchmarks.

* **Open-source.** We take the old saying by heart: “if you want to go fast, go
  alone; if you want to go far, go together”. We believe that by developing
  Velox as a community, there is an opportunity to create a virtuous cycle
  between developers who would like to leverage state-of-the-art execution in
  their systems; and developers, researchers, and hardware vendors who would
  like their techniques and hardware platforms to be pervasively adopted in the
  industry.

## Velox Values

* **High quality.** The development process in Velox is optimized for ensuring
  (a) reliability and high-quality, and (b) ease of maintenance in the long
  term, not for the productivity of the developer adding the feature.

* **Test and verification.** Every change in Velox should be accompanied by
  tests, and a summary describing how the correctness of the change can be
  verified. Oftentimes, extensions to Velox’s automated testing infrastructure
  (fuzzers) should also be added by the author to further validate correctness.

* **Readability.** As a library, Velox coding is optimized for the reader, not
  the writer. We appropriately add comments to our code, document APIs, and
  produce [documentation](https://facebookincubator.github.io/velox/) about the
  main library components. It might take longer to write, but eases maintenance
  and other enhancements in the long run.

* **Code consistency.** The Velox codebase favors consistency over personal
  preference. When contributing code, first review and follow the
  [CONTRIBUTING.md](https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md)
  guidelines, and strictly adhere to our
  [CODING_STYLE.md](https://github.com/facebookincubator/velox/blob/main/CODING_STYLE.md)
  document.

* **Data-driven.** As an execution engine, there is a large number of
  optimizations that can be implemented to improve performance. We prioritize
  the ones that have clear data (in the form of benchmarks based on real-world
  use cases) supporting their efficacy.

* **Adaptivity.** Exposing too many configuration knobs to users increases the
  API complexity, and makes it more error-prone. As much as possible, we try to
  make the library self-adapt to find optimal execution configurations.

