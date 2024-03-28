# PrestoDB: Mission and Architecture

## Introduction

PrestoDB is an open-source distributed SQL query engine designed for fast analytic queries against data sources of all sizes, ranging from gigabytes to petabytes. It was originally developed by Facebook and later made available to the broader community.  It is governed under the auspices of the Presto Foundation, a member of the Linux Foundation.

## Mission

### For users

The primary mission of PrestoDB is to enable efficient, high-speed data processing for analytics and batch at scale. It aims to provide a single, unified query system that can access and process data stored in various formats and storage systems. Key aspects of its mission include:

1. **High Performance:** Presto strives to exceed data warehouse performance over data lakes.  To achieve this, Presto works to achieve vertical integration over modular portions of the stack.
2. **Scalability:** Presto is used at extremely large data lakes across the world.  As such, it aims for high degrees of scalability.
3. **Connectivity and Integration:** Presto is designed to be flexible and adaptable to changes in infrastructure.  This includes a highly customizable plugin infrastructure, including pluggable storage connectors.
4. **User-Friendly:** Presto strives to be simple enough for a user to spin up and quickly test out, using familiar ANSI style SQL syntax, sensible default values and an optimizer that produces efficient query plans.

### For developers

Presto aims to accomplish the above goals for users by means of creating a broad, powerful and collaborative open source community that strives for high standards in database engineering and design.

1. **Open Source:** Develop and maintain as an open-source project, encouraging community contributions and collaboration.
2. **Broad and open community:** Foster a large and powerful community to drive the project’s adoption and direction.
3. **Composability**: Presto is designed for lakes and lakehouses.  As such, it is engineered to fit cohesively in an ecosystem of other tools, and where possible it strives to integrate with industry standard specifications and libraries that help accomplish the user goals above.
4. **Coding excellence**: Presto strives to maintain a high bar for contributions.

## Current Architecture

Presto follows a distributed system model with a coordinator and multiple worker nodes. [See Presto Concepts for more information.](https://prestodb.io/docs/0.285.1/overview/concepts.html#server-types)

Long term initiatives in Presto [have corresponding project boards](https://github.com/prestodb/presto/projects?query=is%3Aopen).  Current progress can be roughly understood by looking at the project boards.

## Long Term Vision

### Vertical integration in data lakes

Presto aims to be the top performing system for data lakes.  The top priority for the project is to move fully onto a native evaluation engine, particularly Velox, with the same expectations outlined in the vision for users, particularly user-friendliness and connectivity.

Motivations for this effort are numerous:

1. It is expected to be too difficult to achieve Warehouse performance using a Java evaluation engine.  Some optimizations, such as vectorized execution, in particular require flexibility and precision which is difficult to achieve in Java.
2. The Presto community is delegating evaluation to a different community, thereby focusing efforts on improved usability.  We believe that shared components can offer network benefits that closed systems may find difficult to achieve, and such a division of efforts will result in a faster and more robust system overall.

### State-of-the-art Optimizer

Presto’s optimizer can learn from long established techniques in more mature systems to provide better plans by default for users.  We are working on bringing these mature optimization techniques into Presto.

### Modularity

Originally, the way that Presto interacted with other systems was unique and proprietary to Presto.  With libraries like Velox and DataFusion standardizing execution, Ibis standardizing data frames, Substrait standardizing an IR, and Arrow standardizing data exchange, there is the possibility of a future that emphasizes more interoperability with other data infrastructure.  We believe that if Presto does not adapt to this future, it stands to be left behind.  Moreover, we are first movers in this area with our early usage of Velox.  As a matter of principle and design, we are aligned in the direction of interoperability, and believe this can be both a differentiator for the project, and allow the project to focus more exclusively on features that make users happy.

### Reliability

Presto is used at some of the largest data lakes in the world.  To support this mission, it must be extremely reliable.  This means that investments in testing infrastructure must always be made when required.
