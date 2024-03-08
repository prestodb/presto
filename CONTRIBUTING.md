# Contributing to Presto

Thanks for your interest in Presto.  Our goal is to build a fast, scalable and reliable distributed SQL query engine for running low latency interactive and batch analytic queries against data sources of all sizes ranging from gigabytes to petabytes.

## Requirements

* Mac OS X or Linux
* Java 8 Update 151 or higher (8u151+), 64-bit. Both Oracle JDK and OpenJDK are supported.
* Maven 3.3.9+ (for building)
* Python 2.4+ (for running with the launcher script)

## Getting Started

Presto's [open issues are here](https://github.com/prestodb/presto/issues). Tag issues that would make a good first pull request for new contributors with [good first issue](https://github.com/prestodb/presto/labels/good%20first%20issue) tag. An easy way to start helping the project is to *file an issue*. Issues can include bugs, new features, or documentation that looks outdated. For community support, [ask for help in Slack](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM).

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

To build the Presto docs, see the [docs README](presto-docs/README.md).

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

Contributions to Presto should be made in the form of GitHub pull request (PR) submissions and reviews.

For a PR to be eligible to be merged, a committer for the appropriate code must review and approve the code. Once approved by a committer, the PR may be merged by anyone.

Pull request reviews are encouraged for anyone in the community who would like to contribute to Presto, and are
expected from contributors and committers in at least equal proportion to their code contributions.

Contributions should have an associated GitHub issue.
* Large changes should have an [RFC](https://github.com/prestodb/rfcs). The [RFC](https://github.com/prestodb/rfcs) should be reviewed before patches are submitted.
* Medium size changes should have an issue. Work from RFCs can be broken down into smaller issues, and those smaller issues should link to the RFC.
* Smaller changes, such as minor bug fixes and code formatting, may not need an issue and can submit a PR without one.

## Minimum Expectations for Contributing to Presto
To commit code, you should:
* Work through the [Getting Started](https://prestodb.io/getting-started/) materials
* [Read and agree to the Code of Conduct](https://github.com/prestodb/tsc/blob/master/CODE_OF_CONDUCT.md)
* [Sign the Presto CLA](https://github.com/prestodb/presto/blob/master/CONTRIBUTING.md#contributor-license-agreement-cla)
* [Join the Presto Slack](https://communityinviter.com/apps/prestodb/prestodb)
* [File an issue](https://github.com/prestodb/presto/issues/new/choose)
* Self-verify new code matches [codestyle](#code-style) for Presto
* Follow the [commit standards](#commit-standards)
* Add or modify existing tests related to code changes being submitted
* Run and ensure that local tests pass before submitting a merge request

## Designing Your Code
* Consider your code through 3 axes
    1. Code Quality and Maintainability, for example:
        1. Code Style
           * Does the code in this PR follow existing coding conventions as outlined in this doc and elsewhere in the code?
        1. Maintainability
           * Is the feature added in a hacky way? If we did everything this way, would the
             code base become hard to use?
           * Is the feature implemented in a clean way with appropriate interfaces that fits
             in with the rest of Presto's design and keeps our code base maintainable?
    1. Code Safety
        1. Adequate testing
        1. Feature flags to enable or disable features that might be risky
        1. Thread safe
        1. No data structures that can grow without bounds; Memory usage is accounted for
        1. Not introducing expensive calls in a performance sensitive area
    1. User friendliness
        1. Config options have names and descriptions that can be understood by someone configuring Presto
        1. All new language features, new functions, and major features have documentation added
        1. Release notes following the [Release Note Guidelines](https://github.com/prestodb/presto/wiki/Release-Notes-Guidelines) are added for user visible changes
* For large features, discuss your design with relevant code owners before you start implementing it.


## Code Style

We recommend you use IntelliJ as your IDE. The code style template for the project can be found in the [codestyle](https://github.com/airlift/codestyle) repository along with our general programming and Java guidelines. In addition to those you should also adhere to the following:

* **Naming**
    * Avoid abbreviations, for example, `positionCount` instead of `positionCnt`
    * Line width and spacing
      * Lines should be no more than 180 chars. Intellij “Reformat code” does NOT enforce the line width, so you must adjust it yourself.
      * Function declarations greater than 180 characters shall be broken down into multiple lines, one argument or parameter per line. A good example is like:

      ```java
          public ParquetPageSource(
                  ParquetReader parquetReader,
                  List<Type> types,
                  List<Optional<Field>> fields,
                  List<String> columnNames,
                  RuntimeStats runtimeStats)
      
      ```

        * Do not put the first parameter on the same line as the function name, or move the parentheses to different lines. Bad example:

      ```java
          public ParquetPageSource(ParquetReader parquetReader,
                  List<Type> types,
                  List<Optional<Field>> fields,
                  List<String> columnNames,
                  RuntimeStats runtimeStats)
      
      ```

        * Group lines of logical units together, and use a single empty line to space out the sections.

      ```java
      public ParquetPageSource(
                ParquetReader parquetReader,
                List<Type> types,
                List<Optional<Field>> fields,
                List<Boolean> rowIndexLocations,
                List<String> columnNames,
                RuntimeStats runtimeStats)
        {
            this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
            this.rowIndexLocations = requireNonNull(rowIndexLocations, "rowIndexLocations is null");
            this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
            this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
      
            checkArgument(
                    types.size() == rowIndexLocations.size() && types.size() == fields.size(),
                    "types, rowIndexLocations, and fields must correspond one-to-one-to-one");
      
            Streams.forEachPair(
                    rowIndexLocations.stream(),
                    fields.stream(),
                    (isIndexColumn, field) -> checkArgument(
                            !(isIndexColumn && field.isPresent()),
                            "Field info for row index column must be empty Optional"));
        }
      ```

* **Ordering of class members and methods**
    * Class members are in front of methods.
    * Group and order the class members and methods in the order of their access levels in descending order:
        1. public
        1. protected
        1. package private
        1. private
    * Group and order the class members in descending order:
        1. static final
        1. final
        1. normal
    * Order the methods with the same access level in the order they’re called.
* **Encourage proper encapsulation**
    * Do not use public class members. Use lower access levels as much as possible. Exceptions may apply for performance critical paths, in that case discuss your design first with relevant code owners.
    * Make a function access level as low as possible
    * If a public class is only used by one caller and the usage is local, consider making it a nested class.
* **Immutable and thread safe classes**
    * Whenever possible, class fields should be final
    * When it's not possible, ensure that accesses to the non-final fields are thread safe by whatever methods are appropriate for the circumstance (Concurrent collections, synchronized access, etc.) if they are in a code path that is multi-threaded

* **Static imports**
  For example, in your code, do not use something like

    ```java
    String.format("%s", rule.getClass().getSimpleName())
    ```

  * Instead, static import this method and use “format()”

  ```java
  import static java.lang.String.format;
  ...
  format("%s", rule.getClass().getSimpleName()) 
  ```
    
  * Similarly, do NOT use
    
  ```java
  time0Bto100KB.add(nanos, TimeUnit.NANOSECONDS);
  ```
    
  * But static import it and use NANOSECONDS in the code.
    
  ```java
  import static java.util.concurrent.TimeUnit.NANOSECONDS;
  ...
  time0Bto100KB.add(nanos, NANOSECONDS);
  ```
    
  * Prefer Immutable collections in Guava when possible. For example, instead of using
    
  ```java
  expressions.stream()
             .map(OriginalExpressionUtils::castToExpression)
             .collect(Collectors.toList())
  ```
    
  * Use ```toImmutableList()```
    
  ```java
  import com.google.common.collect.ImmutableList;
  ...
  import static com.google.common.collect.ImmutableList.toImmutableList;
  ...
  expressions.stream()
             .map(OriginalExpressionUtils::castToExpression)
             .collect(toImmutableList())
  ```

* **Method argument validation**
    * Usually needed for constructors

      ```java
      protected SqlScalarFunction(Signature signature)
      {
          this.signature = requireNonNull(signature, "signature is null");
          checkArgument(signature.getKind() == SCALAR, "function kind must be SCALAR");
      }
      ```

* **Use proper annotations**
    * @Nullable
    * @VisibleForTesting
    * @Override
    * @Experimental
* **Use of Optional instead of bare metal nullable objects when appropriate**
    * public method parameters
    * Performance NON-CRITICAL path
    * Creating Optional objects is not free. If the function call is in a critical loop, do NOT use Optional parameters.
* **Alphabetize**
    * Sections in documentation
    * Methods
    * Variables
    * Sections
* **Comment styles**
    * Add the “/** */” (Javadoc) style comments to all interface methods, with explanation of the parameters and returns. For example:

      ```java
          interface Transformation<T, R>
          {
              /**
               * Processes input elements and returns current transformation state.
               *
               * @param elementOptional an element to be transformed. Will be empty
               * when there are no more elements. In such case transformation should
               * finish processing and flush any remaining data.
               * @return the current transformation state, optionally bearing a result
               * @see TransformationState#needsMoreData()
               * @see TransformationState#blocked(ListenableFuture)
               * @see TransformationState#yield()
               * @see TransformationState#ofResult(Object)
               * @see TransformationState#ofResult(Object, boolean)
               * @see TransformationState#finished()
               */
              TransformationState<R> process(Optional<T> elementOptional);
          }
      ```

    * Recommend to add the “/** */” style comments to important or difficult public methods, with explanation of the parameters and returns.
    * Within the method body, use “//” style comments on separate lines for difficult parts.
    * Use “//” style comments for class members if it can help understanding the code. Can be on the same line or separate lines in front.
* **Code succinctness**
    * Inline function calls when appropriate. For example, if a function is only called once, we don’t need to create a variable for it.
    * Use reference operator in lambda expressions where possible. For example, instead of doing this:

      ```java
      Iterables.getOnlyElement(argumentSets).stream()
      .map((rowExpression) -> OriginalExpressionUtils.castToExpression(rowExpression))
      ```
      Do this:
      ```java
      Iterables.getOnlyElement(argumentSets).stream()
      .map(OriginalExpressionUtils::castToExpression)
      ```

* When appropriate use Java 8 Stream API
* Categorize errors when throwing an exception
* **Tests**
    * Avoid adding `Thread.sleep` in tests--these can fail due to environmental conditions, such as garbage collection or noisy neighbors in the CI environment.


## Commit Standards
* ### Commit Size
    * Recommended lines of code should be no more than +1000 lines, and should focus on one single major topic.\
      If your commit is more than 1000 lines, consider breaking it down into multiple commits, each handling a specific small topic.
* ### Commit Message Style
    * **Separate subject from body with a blank line**
    * **Subject**
        * Limit the subject line to 10 words or 50 characters
        * If you cannot make the subject short, you may be committing too many changes at once
        * Capitalize the subject line
        * Do not end the subject line with a period
        * Use the imperative mood in the subject line
    * **Body**
        * Wrap the body at 72 characters
        * Use the body to explain what and why versus how
        * Use the indicative mood in the body\
          For example, “If applied, this commit will ___________”
        * Communicate only context (why) for the commit in the subject line
        * Use the body for What and Why
          * If your commit is complex or dense, share some of the How context
        * Assume someone may need to revert your change during an emergency
    * **Content**
        * **Aim for smaller commits for easier review and simpler code maintenance**
        * All bug fixes and new features must have associated tests
        * Commits should pass tests on their own, not be dependent on other commits
        * The following is recommended:
            * Describe why a change is being made.
            * How does it address the issue?
            * What effects does the patch have?
            * Do not assume the reviewer understands what the original problem was.
            * Do not assume the code is self-evident or self-documenting.
            * Read the commit message to see if it hints at improved code structure.
            * The first commit line is the most important.
            * Describe any limitations of the current code.
            * Do not include patch set-specific comments.

Details for each point and good commit message examples can be found on https://wiki.openstack.org/wiki/GitCommitMessages#Information_in_commit_messages

* **Metadata**
    * If the commit was to solve a Github issue, refer to it at the end of a commit message in a rfc822 header line format.\
      For example,\
      Resolves: #1234\
      See also: #1235, #1236

* **Backport Commits**
    * Must refer to original commit ID (refer to the PR link in the PR message) or link and author in the commit message
      ```bash
      Fix OOM caused by foo in bar
      
      Foo was pack ratting ByteBuffers causing OOM.
      
      Cherry-pick of https://github.com/prestodb/presto/pull/18424/commits/30e36dae0cdb9debd991931370b86301cd17e261 
      Co-authored-by: Foo Bar <foo@bar.com>
      ```
    * Try to follow the original PR and commits structure. Do not mingle multiple original commits or reorder them.
    * Do not add big modifications in the same backported commit, unless it’s the minimum required to resolve conflicts.
    * If needed, add new separate commits following the backport commit for necessary changes.



## Committers

Presto committers are defined as [code owners](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners) and documented in the project's [`CODEOWNERS`](CODEOWNERS) file.  Each line in the `CODEOWNERS` file defines a module or submodule that the committer has the rights to approve.  New modules and submodules for CODEOWNERS may be added as needed.

New committers are approved by majority vote of the TSC ([see TSC charter](https://github.com/prestodb/tsc/blob/master/CHARTER.md)).  To become a committer, reach out to an [existing TSC member](https://github.com/prestodb/tsc#members) and ask for their feedback on your eligibility (see: [How to become a Presto Committer?](https://github.com/prestodb/presto/wiki/How-to-become-a-Presto-committer%3F)).  Note: to expedite the process, consider creating a document that outlines your Github stats, such as the number of reviews, lines of code added, number of PRs, and outlines particularly outstanding code and review contributions.  If the TSC member believes you are eligible, they will submit your nomination to a vote by the TSC, typically in the form of a PR that adds your handle to the `CODEOWNERS` file.  The process is complete once the PR is merged.

## Pull Requests
* #### PR size and structure
    * A PR can consist of multiple small commits, preferably not more than 20.
    * The total number of lines modified in a single PR shall not exceed 5000. An exception to this rule is for changes that include checked in code generated files (such as [presto_protocol.cpp](https://github.com/prestodb/presto/blob/master/presto-native-execution/presto_cpp/presto_protocol/presto_protocol.cpp)).
    * The commits sequence shall be in the dependencies order, not in date created/modified order
    * Every commit in the PR shall pass all the tests
      Example: https://github.com/prestodb/presto/pull/12991/commits

* #### Follow the PR template provided
* #### Backport PR
    * Must refer to the original PR and provide link in the PR message.
    * Must refer to original authors in the PR message.
    * Do NOT change titles or messages of the original commits.
    * If the original PR contains multiple commits, follow the same structure.


## Pull Request Checklist

If you are not an official committer, you must fork from Master and then submit a Pull Request to contribute as an individual.

To make a contribution:

We use the [Fork and Pull model](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/getting-started/about-collaborative-development-models#fork-and-pull-model)
- Fork from the master branch
- If needed, rebase to the current master
  branch before submitting your pull request
- If it doesn't merge cleanly with
  master you may be asked to rebase your changes.
- If your pull request does not have a reviewer assigned to it after 4 days, [ask for a review in the #dev channel in Slack](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM).

- Tests are expected for all bug fixes and new features.

- Make sure your code follows the [code style guidelines](https://github.com/prestodb/presto/blob/master/CONTRIBUTING.md#code-style), [development guidelines](https://github.com/prestodb/presto/wiki/Presto-Development-Guidelines#development) and [formatting guidelines](https://github.com/prestodb/presto/wiki/Presto-Development-Guidelines#formatting)

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
    * Ensure all code is peer reviewed within your own organization or peers before submitting
    * Implement and address existing feedback before requesting further review
    * Make a good faith effort to locate example or referential code before requesting someone else direct you towards it
    * If you see a lack of documentation, create a separate commit with draft documentation to fill the gap
        * This documentation can be iterated on same as any code commit, demonstrate in real time that you are learning the code section
    * Implement or modify relevant tests, otherwise provide clear explanation why test updates were not necessary
    * Tag your PR with affected code areas as best as you can, it’s okay to tag too many, better to cut down irrelevant tags than miss getting input from relevant subject matter experts

### What not to do for Pull Requests
* Submit before getting peer review in your own organization
* Request review without addressing or implementing previous feedback
* Ask reviewers to provide examples or code references without trying to find them on your own
* Protest lack of documentation for a code section
    * Instead, review the related code, then draft initial documentation as a separate commit
* Submit without test cases or clear justification for lack thereof

## Code Reviews
#### What to do
* Provide explicit feedback on what is needed or what would be better
* Review code with the objective of helping someone land their changes
#### What not to do
* Treat reviews as purely a hunt for mistakes

## Conduct

Please refer to our [Code of Conduct](https://github.com/prestodb/tsc/blob/master/CODE_OF_CONDUCT.md).

## Contributor License Agreement ("CLA")

To accept your pull request, you must submit a CLA. You only need to do this once, so if you've done this for one repository in the [prestodb](https://github.com/prestodb) organization, you're good to go. When you submit a pull request for the first time, the communitybridge-easycla bot notifies you if you haven't signed, and provides you with a link. If you are contributing on behalf of a company, you might want to let the person who manages your corporate CLA whitelist know they will be receiving a request from you.

## License

By contributing to Presto, you agree that your contributions will be licensed under the [Apache License Version 2.0 (APLv2)](LICENSE).
