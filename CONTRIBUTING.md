# Contributing to Presto

Thanks for your interest in Presto.  Our goal is to build a fast, scalable and reliable distributed SQL query engine for running low latency interactive and batch analytic queries against data sources of all sizes ranging from gigabytes to petabytes.

## Requirements

* Mac OS X or Linux
* Java 8 Update 151 or higher (8u151+), 64-bit. Both Oracle JDK and OpenJDK are supported.
* Maven 3.3.9+ (for building)
* Python 2.4+ (for running with the launcher script)

## Getting Started

Presto's [open issues are here](https://github.com/prestodb/presto/issues). Issues that would make a good first pull request for new contributors with the `beginner-task` tag. An easy way to get started helping the project is to *file an issue*. Issues can include bugs, new features, or documentation that looks outdated. For community support, [ask for help in Slack](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM).

<details> <!-- from: https://github.com/prestodb/presto/blob/master/README.md -->
  <summary><h2>Building Presto</h2></summary>

### Overview (Java)

Presto is a standard Maven project. Simply run the following command from the project root directory:

    ./mvnw clean install

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

Presto has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    ./mvnw clean install -DskipTests

After building Presto for the first time, you can load the project into your IDE and run the server. We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). Because Presto is a standard Maven project, you can import it into your IDE using the root `pom.xml` file. In IntelliJ, choose Open Project from the Quick Start box or choose Open from the File menu and select the root `pom.xml` file.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that a 1.8 JDK is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 8.0 as Presto makes use of several Java 8 language features

Presto comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:

* Main Class: `com.facebook.presto.server.PrestoServer`
* VM Options: `-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties`
* Working directory: `$MODULE_WORKING_DIR$` or `$MODULE_DIR$`(Depends your version of IntelliJ)
* Use classpath of module: `presto-main`

The working directory should be the `presto-main` subdirectory. In IntelliJ, using `$MODULE_DIR$` accomplishes this automatically.

Additionally, the Hive plugin must be configured with location of your Hive metastore Thrift service. Add the following to the list of VM options, replacing `localhost:9083` with the correct host and port (or use the below value if you do not have a Hive metastore):

    -Dhive.metastore.uri=thrift://localhost:9083

### Using SOCKS for Hive or HDFS

If your Hive metastore or HDFS cluster is not directly accessible to your local machine, you can use SSH port forwarding to access it. Setup a dynamic SOCKS proxy with SSH listening on local port 1080:

    ssh -v -N -D 1080 server

Then add the following to the list of VM options:

    -Dhive.metastore.thrift.client.socks-proxy=localhost:1080

### Running the CLI

Start the CLI to connect to the server and run SQL queries:

    presto-cli/target/presto-cli-*-executable.jar

Run a query to see the nodes in the cluster:

    SELECT * FROM system.runtime.nodes;

In the sample configuration, the Hive connector is mounted in the `hive` catalog, so you can run the following queries to show the tables in the Hive database `default`:

    SHOW TABLES FROM hive.default;

### Building the Documentation

To learn how to build the docs, see the [docs README](presto-docs/README.md).

### Building the Web UI

The Presto Web UI is composed of several React components and is written in JSX and ES6. This source code is compiled and packaged into browser-compatible JavaScript, which is then checked in to the Presto source code (in the `dist` folder). You must have [Node.js](https://nodejs.org/en/download/) and [Yarn](https://yarnpkg.com/en/) installed to execute these commands. To update this folder after making changes, simply run:

    yarn --cwd presto-main/src/main/resources/webapp/src install

If no JavaScript dependencies have changed (i.e., no changes to `package.json`), it is faster to run:

    yarn --cwd presto-main/src/main/resources/webapp/src run package

To simplify iteration, you can also run in `watch` mode, which automatically re-compiles when changes to source files are detected:

    yarn --cwd presto-main/src/main/resources/webapp/src run watch

To iterate quickly, simply re-build the project in IntelliJ after packaging is complete. Project resources will be hot-reloaded and changes are reflected on browser refresh.

## Presto native and Velox

[Presto native](https://github.com/prestodb/presto/tree/master/presto-native-execution) is a C++ rewrite of Presto worker. [Presto native](https://github.com/prestodb/presto/tree/master/presto-native-execution) uses [Velox](https://github.com/facebookincubator/velox) as its primary engine to run presto workloads.

[Velox](https://github.com/facebookincubator/velox) is a C++ database library which provides reusable, extensible, and high-performance data processing components.

Check out [building instructions](https://github.com/prestodb/presto/tree/master/presto-native-execution#building) to get started.


<hr>
</details>

## Contributions

Presto welcomes contributions from everyone.

Contributions to Presto should be made in the form of GitHub pull request submissions and reviews. 

Each pull request submission will be reviewed by a contributor or committer in the project.  In order for a PR to be eligible to be merged, a committer for the appropriate code must approve the code. Once approved by a committer, the PR may be merged by anyone. 

Pull request reviews are encouraged for anyone in the community who would like to contribute to Presto, and are
expected from contributors and committers in at least equal proportion to their code contributions.

Large contributions should have an associated GitHub issue.

## Code Style

We recommend you use IntelliJ as your IDE. The code style template for the project can be found in the [codestyle](https://github.com/airlift/codestyle) repository along with our general programming and Java guidelines. In addition to those you should also adhere to the following:

* Alphabetize sections in the documentation source files (both in table of contents files and other regular documentation files). In general, alphabetize methods/variables/sections if such ordering already exists in the surrounding code.
* When appropriate, use the Java 8 stream API. However, note that the stream implementation does not perform well so avoid using it in inner loops or otherwise performance sensitive sections.
* Categorize errors when throwing exceptions. For example, PrestoException takes an error code as an argument, `PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS)`. This categorization lets you generate reports so you can monitor the frequency of various failures.
* Ensure that all files have the appropriate license header; you can generate the license by running `mvn license:format`.
* Consider using String formatting (printf style formatting using the Java `Formatter` class): `format("Session property %s is invalid: %s", name, value)` (note that `format()` should always be statically imported). Sometimes, if you only need to append something, consider using the `+` operator.
* Avoid using the ternary operator except for trivial expressions.
* Use an assertion from Airlift's `Assertions` class if there is one that covers your case rather than writing the assertion by hand. Over time we may move over to more fluent assertions like AssertJ.
* When writing a Git commit message, follow these [guidelines](https://chris.beams.io/posts/git-commit/).

## Committers

Presto committers are defined as [code owners](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners) and documented in the project's [`CODEOWNERS`](CODEOWNERS) file.  Each line in the `CODEOWNERS` file defines a module or submodule that the committer has the rights to approve.  New modules and submodules for CODEOWNERS may be added as needed.

New committers are approved by majority vote of the TSC ([see TSC charter](https://github.com/prestodb/tsc/blob/master/CHARTER.md)).  To become a committer, reach out to an [existing TSC member](https://github.com/prestodb/tsc#members) and ask for their feedback on your eligibility (see: [How to become a Presto Committer?](https://github.com/prestodb/presto/wiki/How-to-become-a-Presto-committer%3F)).  Note: to expedite the process, consider creating a document that outlines your Github stats, such as the number of reviews, lines of code added, number of PRs, and outlines particularly outstanding code and review contributions.  If the TSC member believes you are eligible, they will submit your nomination to a vote by the TSC, typically in the form of a PR that adds your handle to the `CODEOWNERS` file.  The process is complete once the PR is merged.

## Pull Request Checklist

- Branch from the master branch and, if needed, rebase to the current master
  branch before submitting your pull request. If it doesn't merge cleanly with
  master you may be asked to rebase your changes.

- If your pull request does not have a reviewer
  assigned to it after a few days, [ask for a review in the #dev channel in Slack](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM).

- Tests are expected for all bug fixes and new features.

- Make sure your code follows the [code style guidelines](https://github.com/prestodb/presto#code-style), [development guidelines](https://github.com/prestodb/presto/wiki/Presto-Development-Guidelines#development) & [formatting guidelines](https://github.com/prestodb/presto/wiki/Presto-Development-Guidelines#formatting)

- Make sure you follow the [review and commit guidelines](https://github.com/prestodb/presto/wiki/Review-and-Commit-guidelines), in particular:

    - Ensure that each commit is correct independently. Each commit should compile and pass tests.
    - When possible, reduce the size of the commit for ease of review.
    - Squash all merge commits before the PR is rebased and merged.
    - Make sure commit messages [follow these guidelines](https://chris.beams.io/posts/git-commit/).  In particular (from the guidelines):

        * Separate subject from body with a blank line
        * Limit the subject line to 50 characters
        * Capitalize the subject line
        * Do not end the subject line with a period
        * Use the imperative mood in the subject line
        * Wrap the body at 72 characters
        * Use the body to explain what and why vs. how
     
- Example commit message

```
Fix OOM caused by foo in bar

Foo was pack ratting ByteBuffers causing OOM.

Cherry-pick of c2bf3b0 (https://github.com/foo/bar/pull/123/commits/c2bf3b025ff456921c4c5ff4bcb814740e823061)

Co-authored-by: Foo Bar <foo@bar.com>
```

- Example PR message
  - please refer [here](https://github.com/prestodb/presto/blob/master/pull_request_template.md)

## Conduct

Please refer to our [Code of Conduct](https://github.com/prestodb/tsc/blob/master/CODE_OF_CONDUCT.md).

## Contributor License Agreement ("CLA")

In order to accept your pull request, we need you to submit a CLA. You only need to do this once, so if you've done this for one repository in the [prestodb](https://github.com/prestodb) organization, you're good to go. If you are submitting a pull request for the first time, the communitybridge-easycla bot will notify you if you haven't signed, and will provide you with a link.  If you are contributing on behalf of a company, you might want to let the person who manages your corporate CLA whitelist know they will be receiving a request from you.

## License

By contributing to Presto, you agree that your contributions will be licensed under the [Apache License Version 2.0 (APLv2)](LICENSE).