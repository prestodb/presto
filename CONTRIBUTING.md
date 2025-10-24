# Contributing to Presto

Thanks for your interest in Presto. Our goal is to build a fast, scalable, and reliable distributed SQL query engine for running low latency interactive and batch analytic queries against data sources of all sizes ranging from gigabytes to petabytes.

# What Would You Like to Do?

| Area             | Information                                                                                                                                                                                                                                                                                                                                    |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Getting Started  | 1. [Build Presto](README.md#user-content-building-presto) <br/>2. Look for [good first issue](https://github.com/prestodb/presto/labels/good%20first%20issue) tickets. <br/> 3. Reference [project boards](https://github.com/prestodb/presto/projects?query=is%3Aopen) for open work.                                                         |
| Report Bug       | To report a bug, visit Presto's [open issues](https://github.com/prestodb/presto/issues).                                                                                                                                                                                                                                                      |
| Contributions    | Please read the [contributions](#contributions) section to learn about how you can contribute to Presto, including the submission process, minimum expectations, and guidelines for designing your code.  Ready to open a pull request? Be sure to review the [Pull Request guidelines](#pullrequests).                                        |
| Contributor License Agreement ("CLA") | First-time contributors must sign a CLA. For more information see [Contributor License Agreement ("CLA")](#cla).                                                                                                                                                                                                                               |
| Supporting Users | Reply to questions on the [Slack channel](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM), check Presto's [open issues](https://github.com/prestodb/presto/issues) for user questions, or help with [code reviews](#codereviews). 
| Need help?       | For community support, [ask for help in Slack](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM).                                                                                                                                   |

## <a id="requirements">Requirements</a>

## Presto Community

The Presto community values:

* Politeness and professionalism in all public forums such as GitHub, Slack, and mailing lists.
* Helping those who come to the project with questions, issues, or code.
* Collaboration and teamwork.

We strive to be a welcoming and inclusive community. We believe that a diverse community is a stronger community, and we welcome all who wish to contribute to the project.

## Mission and Architecture

See [PrestoDB: Mission and Architecture](https://github.com/prestodb/presto/blob/master/ARCHITECTURE.md).

## Getting Started

Read Presto's [open issues](https://github.com/prestodb/presto/issues). Tag issues that would make a good first pull request for new contributors with a [good first issue](https://github.com/prestodb/presto/labels/good%20first%20issue) tag. An easy way to start helping the project is to [open an issue](https://github.com/prestodb/presto/issues/new/choose). Issues can include bugs, new features, or outdated documentation. 
For community support, [ask for help in Slack](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM).

## <a id=contributions>Contributions</a>

Presto welcomes contributions from everyone.

Contributions to Presto should be made in the form of GitHub pull request (PR) submissions and reviews.

For a PR to be eligible to be merged, a committer for the appropriate code must review and approve the code. Once approved by a committer, the PR may be merged by anyone.

Pull request reviews are encouraged for anyone in the community who would like to contribute to Presto, and are
expected from contributors and committers in at least equal proportion to their code contributions.

Contributions should have an associated GitHub issue.
* Large changes should have an [RFC](https://github.com/prestodb/rfcs). The [RFC](https://github.com/prestodb/rfcs) should be reviewed before patches are submitted.
* Medium size changes should have an issue. Work from RFCs can be broken down into smaller issues, and those smaller issues should link to the RFC.
* Smaller changes, such as minor bug fixes and code formatting, may not need an issue and can submit a PR without one.
* New SQL functions should follow the [Presto function guidelines](https://github.com/prestodb/presto/blob/master/FUNCTIONS.md). 

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

## Release Process and Version Support

* **Release Cadence**: ~2 months (volunteer dependent)
* **Version Support**: Latest release plus N-1 through N-4 receive critical fixes; N-5+ unsupported
* **Trunk**: Not stable - never use in production
* **Testing**: Extended RC periods with community testing

Details:
* [Release Process Documentation](presto-docs/src/main/sphinx/develop/release-process.rst) - For developers
* [Version Support Guide](presto-docs/src/main/sphinx/admin/version-support.rst) - For administrators

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
        1. All new language features, new functions, session and config properties, and major features have documentation added
        1. When adding a new method to [Plugin.java](https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/Plugin.java), include documentation for the new method in the [Presto Developer Guide](https://prestodb.io/docs/current/develop.html). 
        1. Release notes following the [Release Note Guidelines](https://github.com/prestodb/presto/wiki/Release-Notes-Guidelines) are added for user visible changes
* For large features, discuss your design with relevant code owners before you start implementing it.


## Code Style

### Java
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

* **Ordering of class members**
    * Fields are in front of methods.
    * Group and order the fields and methods in the order of their access levels in descending order:
        1. public
        1. protected
        1. package private
        1. private
    * Group and order the fields in descending order:
        1. static final
        1. final
        1. normal
    * Order the methods with the same access level in the order they’re called.
* **Encourage proper encapsulation**
    * Do not use public class fields for non-constants. Use less visible access levels as much as possible. Exceptions may apply for performance critical paths. In that case, discuss your design first with relevant code owners.
    * Make a method access level as low as possible
    * If a class is only used by one caller and the usage is local, consider making it a nested class.
* **Immutable and thread safe classes**
    * Whenever possible, class fields should be final.
    * When it's not possible, ensure that accesses to the non-final fields are thread safe by whatever methods are appropriate for the circumstance (concurrent collections, synchronized access, etc.) if they are in a code path that is multi-threaded.

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

    * Add the “/** */” style comments to important or difficult public methods, with explanation of the parameters and returns.
    * Within the method body, use “//” style comments on separate lines for difficult parts.
    * Use “//” style comments for class fields if it helps to clarify the code. Can be on the same line or separate lines in front.
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

* When appropriate use Java Stream API
* Categorize errors when throwing an exception
* **Tests**
    * Avoid adding `Thread.sleep` in tests--these can fail due to environmental conditions, such as garbage collection or noisy neighbors in the CI environment.
    * Do not use random values in tests. All tests should be reproducible.
    * Be careful when storing test data in fields because doing so is
      only appropriate for constant, immutable values. Presto tests
      run with TestNG which, unlike JUnit, does not create a new object for
      each test. Shared instance fields can make tests tightly coupled,
      order dependent, and flaky. If you do use instance fields, 
      reinitialize them before each test in a `@BeforeMethod` method,
      and annotate the class with `@Test(singleThreaded = true)`.

### PrestoC++/Prestissimo (presto-native-execution)

The project follows the [coding standards](https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md#coding-best-practices) of the [Velox](https://github.com/facebookincubator/velox) project.

For code formatting a pre-commit hook is used that is installed locally and can fix issues before changes are pushed to the repository.
Please install the [pre-commit](https://pre-commit.com/) tool. Once installed, run `pre-commit run` to scan and fix your staged changes manually, or optionally, install the hook in the local repository by running `pre-commit install` in the project root.
This results in the hook being automatically run on `git commit` executions.

## Commit Standards

### Conventional Commits
We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification for our commit messages and PR titles.

**PR Title Format:**
```
<type>[(scope)]: <description>
```

**Types:** Defined in [.github/workflows/conventional-commit-check.yml](.github/workflows/conventional-commit-check.yml):
* **feat**: New feature or functionality
* **fix**: Bug fix
* **docs**: Documentation only changes
* **refactor**: Code refactoring without changing functionality
* **perf**: Performance improvements
* **test**: Adding or modifying tests
* **build**: Build system or dependency changes
* **ci**: CI/CD configuration changes
* **chore**: Maintenance tasks
* **revert**: Reverting a previous commit
* **misc**: Miscellaneous changes

Note: Each PR/commit should have a single primary type. If your changes span multiple categories, choose the most significant one or consider splitting into separate PRs.

**Scope (optional):** The area of code affected. Valid scopes are defined in [.github/workflows/conventional-commit-check.yml](.github/workflows/conventional-commit-check.yml). Common scopes include:

* `parser` - SQL parser and grammar
* `analyzer` - Query analysis and validation
* `planner` - Query planning, optimization, and rules (including CBO)
* `spi` - Service Provider Interface changes
* `scheduler` - Task scheduling and execution
* `protocol` - Wire protocol and serialization
* `connector` - Changes to broader connector functionality or connector SPI
* `resource` - Resource management (memory manager, resource groups)
* `security` - Authentication and authorization
* `function` - Built-in functions and operators
* `type` - Type system and type coercion
* `expression` - Expression evaluation
* `operator` - Query operators (join, aggregation, etc.)
* `client` - Client libraries and protocols
* `server` - Server configuration and management
* `native` - Native execution engine
* `testing` - Test framework and utilities
* `docs` - Documentation
* `build` - Build system and dependencies

Additionally, any connector name (e.g., `hive`, `iceberg`, `delta`, `kafka`) or plugin name (e.g., `session-property-manager`, `access-control`, `event-listener`) can be used as a scope. These scopes should use the format `plugin-<name>` (e.g., `plugin-iceberg`, `plugin-password-authenticator`).

**Description:**
* Must start with a capital letter
* Must not end with a period
* Use imperative mood ("Add feature" not "Added feature")
* Be concise but descriptive

**Breaking Changes:**
* Use `!` after the type/scope (e.g., `feat!: Remove deprecated API`)
* AND include `BREAKING CHANGE:` in the commit description footer with a detailed explanation of the change
* Use to indicate any change that is not backward compatible as defined in the [Backward Compatibility Guidelines](presto-docs/src/main/sphinx/develop/release-process.rst#backward-compatibility-guidelines)

**Examples:**
* `feat(connector): Add support for dynamic catalog registration` (new feature for connectors)
* `fix: Resolve memory leak in query executor`
* `docs(api): Update REST API documentation`
* `feat!: Remove deprecated configuration options` (breaking change)
* `feat(plugin-iceberg): Add support for Iceberg table properties` (new feature in Iceberg, **NOTE: connectors are plugins**)

### Single Commit PRs
* **All PRs must be merged as a single commit** using GitHub's "Squash and merge" feature
* The PR title will become the commit message, so it must follow the conventional commit format
* Multiple commits within a PR are allowed during development for easier review, but they will be squashed on merge
* If you need to reference other commits or PRs, include them in the PR description or commit body, not as separate commits

### Commit Message Guidelines
* **PR Title/First Line**
    * Must follow conventional commit format
    * Limit to 50-72 characters when possible
    * If you cannot make it concise, you may be changing too much at once

* **PR Description/Commit Body**
    * Separate from title with a blank line
    * Wrap at 72 characters
    * Explain what and why, not how
    * Include:
        * Why the change is being made
        * What issue it addresses
        * Any side effects or limitations
        * Breaking changes or migration notes if applicable
    * Assume someone may need to revert your change during an emergency

* **Content Requirements**
    * All bug fixes and new features must have associated tests
    * Changes should be focused on a single topic
    * Code should pass all tests independently
    * Include documentation updates with code changes

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

Presto has two levels of committers: module committers and project committers.  Presto committers are defined as [code owners](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners) and documented in the project's [`CODEOWNERS`](CODEOWNERS) file, and are either directly referenced in the file, or through a Github team. Each line in the `CODEOWNERS` file defines a module or submodule that the committer or team has the rights to approve.  New modules and submodules for [`CODEOWNERS`](CODEOWNERS)  may be added as needed.

### Baseline expectations from committers

Both module and project committers must demonstrate technical mastery of at least their core areas. They must also show evidence that they are aligned with the project’s community and growth, including the goals outlined in [PrestoDB: Mission and Architecture](ARCHITECTURE.md), and demonstrate kindness and professionalism, help others in the project, and work collaboratively as a team.

### Module committers
#### What is a module committer?

A module committer is a committer who has an entry associated with a module or folder in the project’s [`CODEOWNERS`](CODEOWNERS)  file.

See [`CODEOWNERS`](CODEOWNERS) for modules, connectors, and libraries that lack active module committership. If you have interest in contributing to one of these, work toward becoming a committer for that area.

#### Expectations of a module committer

Module committers have demonstrated mastery in one particular area of the project.  Some examples include:

* A connector or plugin;
* A part of the Presto codebase, such as the optimizer;
* An external component, such as the Java client, or UI.

In addition to technical mastery, they have demonstrated the values of the project through code reviews, design reviews, or responses to questions.  Examples should include many of the following:

* The applicant is known to frequently review pull requests corresponding to the module they are applying for.
* The applicant helps to maintain the module they are applying for when appropriate, such as fixing test cases, adding documentation, fixing bugs, and mentoring others.
* The applicant is known to answer questions on Slack periodically.
* The applicant has provided high quality feedback on Github issues and RFCs.

### Project committers

#### What is a project committer?

A project committer is a committer who has access to approve code across the whole project by membership in the [committers](https://github.com/orgs/prestodb/teams/committers) Github team.

#### Expectations of a project committer

In addition to demonstrating mastery of at least one area of the codebase by becoming a module committer, they have also demonstrated the following:

* They have contributed at least one non-trivial change to the project outside of their core area;
* They exercise great judgment (including deferring to others when appropriate);
* They have experience with reviewing code and making code changes outside of their core area of expertise;
* They set a high bar for their own contributions and those of others during code reviews, including avoiding hacks and temporary workarounds;
* They go above and beyond a module committer in helping maintain the project by regularly reviewing code outside of their area of expertise, or helping users of the project in public channels such as Slack, GitHub, or helping review designs outside of their area of expertise such as providing guidance on Github Issues or RFCs.

Examples should include many of the following:

* The applicant is known to frequently review pull requests outside of the module they maintain.
* The applicant helps to maintain the project, such as by fixing test cases, adding documentation, fixing bugs, and mentoring others.
* The applicant is known to answer questions on Slack periodically.
* The applicant has provided high quality feedback on Github issues and RFCs outside of the module they maintain.

### Voting for committers

New project and module committers are approved by majority vote of the TSC ([see TSC charter](https://github.com/prestodb/tsc/blob/master/CHARTER.md)).  To become a committer, reach out to an [existing TSC member](https://github.com/prestodb/tsc#members), or send an email to operations@prestodb.io, and ask for feedback on your eligibility.  Note: to expedite the process, consider creating a document that outlines your Github stats, such as the number of reviews, lines of code added, number of PRs, and outlines particularly outstanding code and review contributions.  If a TSC member believes you are eligible, they will submit your nomination to a vote by the TSC.  If you receive a majority approval from the vote in the TSC then a pull request will be raised that adds your Github handle to the  [`CODEOWNERS`](CODEOWNERS) file.  The process is complete once the PR is merged.

## <a id="pullrequests">Pull Requests</a>
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

- Make sure you follow the [Commit Standards](#commit-standards) section above, which uses Conventional Commits format:

    - PR titles must follow the conventional commit format (e.g., `feat: Add new feature`, `fix: Resolve bug`)
    - All PRs will be squashed into a single commit on merge, so the PR title becomes the commit message
    - While developing, you can have multiple commits in your PR for easier review
    - Ensure each commit in your PR compiles and passes tests independently
    - The PR description should explain what and why, not how. Keep lines wrapped at 72 characters for better readability. Include context about why the change is needed, what issue it addresses, any side effects or breaking changes, and enough detail that someone could understand whether to revert it during an emergency.
- Ensure all code is peer reviewed within your own organization or peers before submitting
- Implement and address existing feedback before requesting further review
- Make a good faith effort to locate example or referential code before requesting someone else direct you towards it
- If you see a lack of documentation, create a separate commit with draft documentation to fill the gap
        * This documentation can be iterated on same as any code commit, demonstrate in real time that you are learning the code section
- Implement or modify relevant tests, otherwise provide clear explanation why test updates were not necessary
- Tag your PR with affected code areas as best as you can, it’s okay to tag too many, better to cut down irrelevant tags than miss getting input from relevant subject matter experts
- All tests shall pass before requesting a code review. If there are test failures, even it's from unrelated problems, try to address them by either sending a PR to fix it or creating a Github issue so it can be triaged and fixed soon.
- If adding new dependencies:
    * OpenSSF Scorecard: Ensure they have an [OpenSSF Scorecard](https://securityscorecards.dev/#the-checks) score of 5.0 or higher. Dependencies with scores below 5.0 require explicit approval from the TSC. [The OpenSSF score can be checked here](https://scorecard.dev/viewer/). Automated checks will comment on the PR with scorecard scores for new dependencies.
    * Vulnerabilities: Ensure new dependencies do not introduce known high or critical severity vulnerabilities. Automated checks will fail the build if such vulnerabilities are detected. In exceptional cases, this can be overridden by TSC vote, requiring an administrator to merge the PR.

### What not to do for Pull Requests
* Submit before getting peer review in your own organization
* Request review without addressing or implementing previous feedback
* Ask reviewers to provide examples or code references without trying to find them on your own
* Protest lack of documentation for a code section
    * Instead, review the related code, then draft initial documentation as a separate commit
* Use AI tools to generate documentation
* Submit without test cases or clear justification for lack thereof
* Request review when there are tests failing

### Comments in Pull Requests
Comments should help move the PR toward completion. 

Presto PRs - especially those written by people new to open source or new to Presto - can have a surprisingly high number of comments. This is a general tendency of open source projects and is because members of the Presto community want to help you succeed with your PR and also maintain the quality, and follow the existing standards of, the Presto project.

Do not take the presence of many comments as a sign that the PR, or the work in it, is bad. 

## <a id="codereviews">Code Reviews</a>
#### What to do
* Provide explicit feedback on what is needed or what would be better
* Review code with the objective of helping someone land their changes
#### What not to do
* Treat reviews as purely a hunt for mistakes

## Conduct

Please refer to our [Code of Conduct](https://github.com/prestodb/tsc/blob/master/CODE_OF_CONDUCT.md).

## <a id="cla">Contributor License Agreement ("CLA")</a>

To accept your pull request, you must submit a CLA. You only need to do this once, so if you've done this for one repository in the [prestodb](https://github.com/prestodb) organization, you're good to go. When you submit a pull request for the first time, the communitybridge-easycla bot notifies you if you haven't signed, and provides you with a link. If you are contributing on behalf of a company, you might want to let the person who manages your corporate CLA whitelist know they will be receiving a request from you.
