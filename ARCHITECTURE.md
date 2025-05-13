# PrestoDB: Mission and Architecture

## Introduction

PrestoDB is an open-source distributed SQL query engine designed for fast analytic queries against data sources of all sizes, ranging from gigabytes to petabytes. It was originally developed by Facebook and later made available to the broader community. It is governed by the Presto Foundation, a member of the Linux Foundation.

## Mission

### For users

The primary mission of PrestoDB is to enable efficient and high-speed data processing for analytics and batch at scale. It aims to provide a single, unified query system that can access and process data stored in various formats and storage systems. Key aspects of its mission include:

1. **High Performance:** Presto aims to be a single unified query engine suitable for low latency interactive workloads, adhoc and exploratory analytics, and large scale batch workloads. To achieve this, Presto works to achieve vertical integration over modular portions of the stack.
2. **Scalability:** Presto is used to access large amounts of data from data lakes. As such, it aims for high degrees of scalability.
3. **Connectivity and Integration:** Presto is designed to be flexible and adaptable to changes in infrastructure. This includes a highly customizable plugin infrastructure, including pluggable storage connectors.
4. **User-Friendly:** Presto strives to be simple enough for a user to spin up and quickly test out, using familiar ANSI style SQL syntax, sensible default values and an optimizer that produces efficient query plans.

### For developers

Presto aims to accomplish the above goals for users by creating a broad, powerful, and collaborative open source community that strives for high standards in database engineering and design.

1. **Open Source:** Develop and maintain an open-source project, encouraging community contributions and collaboration.
2. **Broad and open community:** Foster a large and active community to drive the project’s adoption and direction.  We believe that Presto should survive any single individual or company, and strive to make the project as diverse as possible among committers and their respective employers.
3. **Composability**: Presto is designed for lakes and lakehouses. As such, it is engineered to fit cohesively in an ecosystem of other tools, and strives to integrate with industry standard specifications and libraries that help accomplish the user goals above.
4. **Coding excellence**: Presto strives to maintain a high bar for contributions through good design, good abstractions, rigorous tests, and quality documentation.

## Presto Community

The Presto project believes that, while excellence in the code is table stakes for the project, of even greater importance is *how* the project develops the code.  For more information, see [Presto Community](https://github.com/prestodb/presto/blob/master/CONTRIBUTING.md#presto-community).

## Presto Technical Architecture

Presto follows a distributed system model with a coordinator and multiple worker nodes. [See Presto Concepts for more information.](https://prestodb.io/docs/current/overview/concepts.html#server-types)

Long term initiatives in Presto [have corresponding project boards](https://github.com/prestodb/presto/projects?query=is%3Aopen). Current progress can be roughly understood by looking at the project boards.

## Long Term Vision

### Vertical integration in data lakes

Presto aims to be the top performing system for data lakes. The top priority for the project is to move fully onto a native evaluation engine, particularly [Velox](https://velox-lib.io/), with the same expectations outlined in the vision for users, emphasizing user-friendliness and connectivity.

Motivations for this effort are numerous:

1. It is expected to be too difficult to achieve data warehouse performance using a Java evaluation engine. Some optimizations, such as vectorized execution in particular, require flexibility and precision which is difficult to achieve in Java.
2. The Presto community delegates evaluation to a different community to focus our efforts on improved usability. We believe that shared components can offer network benefits that closed systems may find difficult to achieve, and such a division of efforts will result in a faster and more robust system overall.

### State-of-the-art Optimizer

Presto’s optimizer can learn from long established techniques in more mature systems to provide better plans for users. We are working on bringing these mature optimization techniques into Presto.

### Modularity

Originally, the way that Presto interacted with other systems was unique and proprietary.  Today, libraries like Velox and DataFusion are standardizing execution, Ibis is standardizing data frames, Substrait is standardizing an intermediate representation of plans, and Arrow is standardizing data exchange. These standardizations point to a future that emphasizes interoperability with other data infrastructure. We believe that if Presto does not adapt to this future, it could be left behind.  

In principle and design, we favor the direction of interoperability. We are first movers in this area with our early usage of Velox. We believe that interoperability can be both a differentiator for the project, and allow the project to focus on features that make users happy.

### Reliability

Presto is used at some of the largest data lakes in the world. To support this mission, it must be extremely reliable. This means that investments in testing infrastructure must always be made when required.
