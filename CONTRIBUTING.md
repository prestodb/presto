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

* When appropriate use Java 8 Stream API
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

## Presto Contributor Ladder

The Presto community has four main roles: Contributor, Module Committer, Project Committer, and Technical Steering Committee (TSC) Member.
Each role has specific requirements, responsibilities, and benefits. The roles are designed to recognize and reward community members who 
maintain the project.

### Roles and Expectations

#### Contributor

Contributors are community members who actively participate by submitting pull requests (PRs), engaging in discussions,
and reviewing code. They are the foundation of the Presto community.  There are no requirements to become a Contributor,
and anyone can contribute to the project by following the guidelines outlined in this document.

Requirements:

* Sign the Contributor License Agreement (CLA).
* Submit pull requests to fix bugs, add features, or improve documentation.
* Participate in discussions on GitHub issues and community forums.
* Review PRs from other contributors.

Responsibilities:

* Make meaningful contributions to the codebase, documentation, or other areas.
* Engage in code reviews to help maintain code quality.
* Collaborate with other contributors and help foster a positive, inclusive community.

Benefits:

* Recognition in release notes and community updates.
* Influence the project’s direction through active contributions and participation.
* Opportunity to advance to more senior roles such as Module Committer or Project Committer.
* Follow the project’s guidelines and values, including those outlined in this document.

#### Module Committer

Module Committers have expertise in a specific module of the Presto codebase and are responsible for maintaining
the quality and stability of that module. They are authorized to merge pull requests related to their specific areas 
and ensure the quality of that module. Module Committers are defined in the project’s [`CODEOWNERS`](CODEOWNERS) file, 
which specifies the modules or submodules they are responsible for.

Requirements:

* Demonstrate technical mastery of a specific module or area of the codebase.
* Consistent, high-quality contributions and reviews to the module over an extended period (usually 6+ months).
* Align with the project’s goals and values, as outlined in the [PrestoDB: Mission and Architecture](ARCHITECTURE.md).
* Be nominated by a Project Committer or Technical Steering Committee (TSC) member, and approved by the existing Committers.

Responsibilities:

* Review and merge pull requests for the modules or submodules they oversee.
* Maintain high standards for their module through code reviews, documentation updates, and mentoring others.
* Ensure the module’s stability, quality, and long-term maintenance.
* Collaborate with other Committers to ensure that their module aligns with the overall project goals.

Examples of mastery areas:

* A connector or plugin.
* A specific part of the Presto codebase, such as the optimizer.
* External components like the Java client or UI.

Benefits:

* Authority to merge PRs for the modules they oversee.
* Recognition as a key leader within their module.
* Opportunities to influence the direction of their specific module.

#### Project Committer

Project Committers have broad authority across the entire Presto codebase and are recognized as core contributors to the 
project . They are responsible for ensuring the quality and direction of the project as a whole, and they have access to 
approve code changes in any area of the project. Project Committers are members of the 
[committers](https://github.com/orgs/prestodb/teams/committers) GitHub team.

Requirements:

* Demonstrate mastery in at least one area by becoming a Module Committer.
* Contribute at least one significant change outside of their core module or area.
* Demonstrate sound judgment, including knowing when to defer decisions to others with more expertise.
* Frequently review and contribute to areas of the codebase outside their core area.
* Set high standards for contributions, ensuring that hacks and temporary workarounds are avoided.

Responsibilities:

* Review and merge pull requests across the entire codebase.
* Ensure that contributions maintain the overall quality, coherence, and sustainability of the project.
* Mentor contributors and Module Committers to help them grow.
* Participate in discussions and decisions about the project’s direction, and review RFCs and other major proposals.
* Regularly contribute to code reviews and engage with the community to solve issues across multiple areas of the project.

Examples of expectations:

* Frequently reviewing pull requests outside their primary area of expertise.
* Helping to maintain the project by fixing test cases, adding documentation, and mentoring others.
* Actively participating in public channels like Slack and GitHub by answering questions and offering guidance.

Benefits:

* Full access to the Presto repository with the ability to merge PRs across all areas.
* Influence over the project’s overall direction and long-term goals.
* Recognition as a core leader of the project.

#### Technical Steering Committee (TSC) Member

Description:

TSC members are the leaders responsible for the overall governance and strategic direction of the Presto project. They ensure 
that the project remains aligned with its long-term goals and community vision.

Requirements:

* Long-term, sustained contributions to the project.
* Deep involvement in key project decisions and strategic planning.
* Experience as a Project Committer with demonstrated leadership.
* Nominated and approved by the current TSC members.

Responsibilities:

* Set the overall strategic direction of the project.
* Resolve disputes within the community and guide governance issues.
* Oversee major releases, set project milestones, and ensure long-term planning.
* Maintain the project’s vision and goals in collaboration with the community.
* Lead the voting process for new Project and Module Committers.

Benefits:

* Full decision-making power on technical and governance issues.
* Recognition as a leader and core decision-maker of the project.
* Direct influence over the project’s future direction and sustainability.

#### Voting for Committers

Both Module and Project Committers are approved through a majority vote by the Technical Steering Committee (TSC) 
(see [see TSC charter](https://github.com/prestodb/tsc/blob/master/CHARTER.md)). To become a committer, reach out to an [existing TSC member](https://github.com/prestodb/tsc#members) or email [operations@prestodb.io](mailto:operations@prestodb.io) for
feedback on your eligibility.

Steps to become a Committer:

1. Prepare a document outlining your contributions to Presto, including your GitHub stats (number of reviews, lines of code added, number of PRs, etc.).
2. If a TSC member believes you are eligible, they will nominate you for a vote.
3. The TSC will vote on your nomination, and if you receive a majority approval, a PR will be raised to add your name to the [`CODEOWNERS`](CODEOWNERS) file.
4. The process is complete once the PR is merged.

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
