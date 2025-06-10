# Contributing

Welcome! Thank you for your interest in the Velox project. Before starting to
contribute, please take a moment to review the guidelines outlined below.

Contributions are not just about code. Contributing code is great, but that’s
probably not the best place to start. There are many ways in which people can
make contributions to the project and community.

## Code of Conduct

First and foremost, the Velox project and all its contributors and
[maintainers]((https://velox-lib.io/docs/community/components-and-maintainers))
are governed by a [Code of Conduct](CODE_OF_CONDUCT.md). When participating,
you are expected to uphold this code.

## Community

A good first step to getting involved in the Velox project is to participate in
conversations in GitHub
[Issues](https://github.com/facebookincubator/velox/issues) and
[Discussions](https://github.com/facebookincubator/velox/discussions), and join
the [the Velox-OSS Slack workspace](http://velox-oss.slack.com) - please
comment on [this
Discussion](https://github.com/facebookincubator/velox/discussions/11348) to
get access.

## Components and Maintainers

Velox is logically organized into components, each maintained by a group of
individuals.  The list of components and their respective maintainers [can be
found here](https://velox-lib.io/docs/community/components-and-maintainers).

## Documentation

Help the community understand how to use the Velox library by proposing
additions to our [docs](https://facebookincubator.github.io/velox/index.html) or pointing
out outdated or missing pieces.

## Bug Reports

Found a bug? Help us by filing an issue on GitHub.

Ensure the bug was not already reported by searching
[GitHub Issues](https://github.com/facebookincubator/velox/issues). If you're
unable to find an open issue addressing the problem, open a new one. Be sure to
include a title and clear description, as much relevant information as
possible, and a code sample or an executable test case demonstrating the
expected behavior.

Meta has a [bounty program](https://www.facebook.com/whitehat/) for the safe disclosure
of security bugs. In those cases, please go through the process outlined on that page
and do not file a public issue.

## Code Contribution Process

The code contribution process is designed to reduce the burden on reviewers and
maintainers, allowing them to provide more timely feedback and keeping the
amount of rework from contributors to a minimum.

We encourage new contributors to start with bug fixes and small features so you
get familiar with the contribution process, while building relationships with
community members.  Look for GitHub issues labeled [good first
issue](https://github.com/facebookincubator/velox/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
or consider adding one of the [missing Presto SQL
functions](https://github.com/facebookincubator/velox/issues/2262).

The contribution process is outlined below:

1. Sign the [Contributor License Agreement](https://code.facebook.com/cla)
   (CLA). This step needs to be completed only once.

2. Review the [`LICENSE`](LICENSE) file. By contributing to Velox, you agree that your
   contributions will be licensed under that LICENSE file. This step needs to be
   completed only once.

3. Start a discussion by creating a Github Issue, or asking on Slack (unless the change is trivial).
   * This step helps you identify possible collaborators and reviewers.
   * Does the proposed change align with the technical vision and project values?
   * Will the change conflict with another change in progress? If so, work with others to minimize impact.

4. Implement the change.
   * Always follow the [coding best practices](#coding-best-practices) outlined below.
   * If the change is large, consider posting a draft Github pull request (PR)
     with the title prefixed with [WIP], and share with collaborators to get early feedback.
   * Ensure the PR follows the [title and description
     guidelines](#commit-messages) presented below.
   * Create/submit a Github PR and make sure it passes **all CI tests**.
     * Do not ignore red CI signals, even if they seem preexisting.
     * If you believe a red CI signal is unrelated to your change, please search for
     existing Issues reporting this particular test. They should contain the test name
     in the Issue title.
     * If an Issue already exist, add a comment containing the link to your failed CI job.
     * If an Issue does not exist, please create one with the title "Broken CI \<test\_name\>",
     tagging the appropriate maintainers for that component.
   * Once all CI signals are green, tag the reviewers identified in Step 3.

5. Review is performed by one or more reviewers.
   * This normally happens within a few days, but may take longer if the change
   is large, complex, or if a critical reviewer is unavailable (feel free to
   ping them in the PR or on Slack).

6. Address feedback and update the PR.
   * After pushing changes, add a comment to the PR mentioning the
   reviewer(s) by name, stating the comments have been addressed. This is the best
   way to ensure that the reviewer is notified that the code is ready to be reviewed
   again.
   * As a PR author, please do not "Resolve Conversation" when review comments are
   addressed. Instead, wait for the reviewer to verify the comment has been
   addressed and resolve the conversation.

7. Iterate on this process until your changes are reviewed and accepted by a
   maintainer. At this point, a Meta employee will be notified to merge your PR,
   due to tooling limitations.

## Commit Messages

We build Velox for the long-run, and to do so it is crucial that project
maintainers are able to efficiently inspect and understand project logs.

Commit messages that follow a strict pattern improve maintainability, and allow
tasks such as summarizing changelogs and identifying API breaking changes to be
automated. Despite requiring more rigor from authors and reviewers, consistency
and uniformity in developer workflows improve productivity in the long term.

In Velox, commit messages are generated based on the input provided to PRs, and
must follow the [conventional commit
specification](https://www.conventionalcommits.org/en/v1.0.0/) in the following
manner:

**PR titles** must follow the pattern:

> \<type\>[(optional scope)]: \<description\>

where:

* *Type* can be any of the following keywords:
  * **feat** when new features are being added.
  * **fix** for bug fixes.
  * **build** for build or CI-related improvements.
  * **test** for adding tests (only).
  * **docs** for enhancements to documentation (only).
  * **refactor** for refactoring (no logic changes).
  * **misc** for other changes that may not match any of the categories above.

* PR titles also take an *optional scope* field containing the area
  of the code being target by the PR, to further help commit classification.
  For example, "fix(expr): " or "refactor(parquet): " or "feat(memory):".
  It is ok to omit this field if there is no adequate classification for the
  PR, or if the PR touches multiple different components.

  * Examples of scopes are *vector, type, expr, operator, memory, dwio,
    parquet, dwrf, filesystem, connector, hive, function, aggregate*, but not
    limited to.

* A *description* sentence summarizing the PR, written in imperative tone. The
  description must be capitalized, not contain a period at the end, and wrap
  lines at the 80 characters limit to improve git-log readability.

 * PR titles should also add a '!' to signal if the PR may break backward
   compatibility. For example: "fix(expr)!: ..." or "feat!: ...". Moreover, the
   compatibility changes need to be described in a section in the PR body as
   described below.

Examples of PR titles are:

* feat(type): Add IPPREFIX
* fix: Prevent unnecessary flatmap to map conversion
* refactor(vector): Use 'if constexpr' for Buffer::is_pod_like_v\<T\>

The **PR body** must contain a summary of the change, focusing on the *what*
and *why*. Wrap lines at 80 characters for better git-log readability.

**Breaking API Changes.** A "BREAKING CHANGE:" footer must be added to PRs
that break backwards compatibility of any external API in Velox, followed by a
sentence explaining the extent of the API change. This means either API changes
that may break client builds, or semantic changes on the behavior of such APIs.

If there is a Github Issue or Discussion related to the PR, authors must also
add a "Fixes #[number]" line to the bottom of the PR body. This instructs
Github to automatically close the associated Issue when the PR is merged. If
you merely want to link the PR to an Issue (without closing it when the PR gets
merged), use the pattern "Part of #[number]".

Before contributing PRs to Velox, please review these resource about how to
write great commit messages:

* [How to Write Better Git Commit Messages – A Step-By-Step Guide](https://www.freecodecamp.org/news/how-to-write-better-git-commit-messages/)
* [How to Write a Git Commit Message](https://cbea.ms/git-commit/)

## Coding Best Practices

When submitting code contributions to Velox, make sure to adhere to the
following best practices:

1. **Coding Style**: Review and strictly follow our coding style document,
   available in [`CODING_STYLE.md`](CODING_STYLE.md).
   * Velox favors consistency over personal preference. If there are
   technical reasons why a specific guideline should not be followed,
   please start a separate discussion with the community to update the
   coding style document first.
   * If you are simply updating code that did not comply with the coding
   style, please do so in a standalone PR isolated from other logic changes.

2. **Small Incremental Changes**: If the change is large, work with the maintainers
   on a plan to break and submit it as smaller (yet atomic) parts.
   * [Research indicates](https://smartbear.com/learn/code-review/best-practices-for-peer-code-review/)
   that engineers can only effectively review up to
   400 lines of code at a time. The human brain can only process so much information
   at a time; beyond that threshold the ability to find bugs and other flaws
   decreases.
   * As larger PRs usually take longer to review and iterate, they
   tend to slow down the software development process. As much as possible,
   split your work into smaller changes.

3. **Unit tests**: With rare exceptions, every PR should contain unit tests
   covering the logic added/modified.
   * Unit tests protect our codebase from regressions, promote less coupled
   APIs, and provide an executable form of documentation that’s useful for
   new engineers reasoning about the codebase.
   * Good unit tests are fast, isolated, repeatable, and exercise all APIs
   including their edge cases.
   * The lack of existing tests is not a good reason not to add tests to
   your PR. If a component or API does not have a corresponding
   unit test suite, please consider improving the codebase by first adding a
   new unit test suite to ensure the existing behavior is correct.

4. **Code Comments**: Appropriately add comments to your code and document APIs.
   * As a library, Velox code is optimized for the reader, not the writer.
   * Comments should capture information that was on the writer’s mind, but
   could not be represented as code. The overall goal is to make the code more
   obvious and remove obscurity.
   * As a guideline, every file, class, member variable, and member function
   that is not a getter/setter should be documented.
   * As much as possible, try to avoid functions with very large bodies. In the
   (rare) cases where large code blocks are needed, a good practice is to group
   smaller blocks of related code, and precede them with a blank line and a
   high-level comment explaining what the block does.

5. **Benchmarks**: Add micro-benchmarks to support your claims.
   * As needed, add micro-benchmark to objectively evaluate performance and
   efficiency trade-offs.

6. **APIs**: Carefully design APIs.
   * As a library, Velox APIs should be intentional. External API should only
   be deliberately created.
   * As a rule of thumb, components should be deep and encapsulate as much
   complexity as possible, and APIs should be narrow, minimizing dependencies
   across components and preventing implementation details from leaking through
   the API.

## Adding Scalar Functions

Adding Presto and Spark functions are a good way to get yourself familiar with
Velox and the code review process. In addition to the general contribution
guidelines presented above, here are specific guidelines for contributing
functions:

1. Read [How to add a scalar function?](https://facebookincubator.github.io/velox/develop/scalar-functions.html) guide. When implementing a function, simple function is preferred unless the implementation of vector function provides a significant performance gain which can be demonstrated
with a benchmark.

2. Use the following template for the PR title: Add xxx [Presto|Spark] function (replace xxx with the function name).
   * Ensure the PR description contains a link to the function documentation
   from Presto or Spark docs.
   * Describe the function semantics and edge cases clearly.

3. Use Presto or Spark to check the function semantics.
   * When implementing a Spark function, check the function semantics using Spark 3.5 with ANSI OFF.
   * Try different edge cases to check whether the function returns null, or
   throws, etc.
   * Make sure to replicate the exact semantics.

4. Add tests exercising common inputs, all possible signatures and corner cases.
   * Make sure the test cases are concise and easily readable.

5. Make sure that obvious inefficiencies are addressed.
   * If appropriate, provide micro-benchmarks to support your claims with data.

4. Add documentation for the new function to an .rst file under velox/docs/functions directory.
   * Functions in documentation are listed in alphabetical order. Make sure to
   place the new function so that the order is preserved.

6. Run [Fuzzer](https://facebookincubator.github.io/velox/develop/testing/fuzzer.html)
   to test new the function in isolation as well as in combination with other functions.
   Note: Fuzzer is under active development and the commands below may not work as is.
   Consult the CircleCI configuration in .circleci/config.yml for the up-to-date command
   line arguments.

   ```
   # Test the new function in isolation. Use --only flag to restrict the set of functions
   # and run for 60 seconds or longer.
   velox_expression_fuzzer_test --only <my-new-function-name> --duration_sec 60 --logtostderr=1 --enable_variadic_signatures --velox_fuzzer_enable_complex_types --velox_fuzzer_enable_decimal_type --lazy_vector_generation_ratio 0.2 --velox_fuzzer_enable_column_reuse --velox_fuzzer_enable_expression_reuse

   # Test the new function in combination with other functions. Do not restrict the set
   # of functions and run for 10 minutes (600 seconds) or longer.
   velox_expression_fuzzer_test --duration_sec 600 --logtostderr=1 --enable_variadic_signatures --velox_fuzzer_enable_complex_types --velox_fuzzer_enable_decimal_type --lazy_vector_generation_ratio 0.2 --velox_fuzzer_enable_column_reuse --velox_fuzzer_enable_expression_reuse
   ```

Here are example PRs:

* [Add sha256 Presto function](https://github.com/facebookincubator/velox/pull/1000)
* [Add sin, cos, tan, cosh and tanh Presto functions](https://github.com/facebookincubator/velox/pull/313)
* [Add transform_keys and transform_values Presto functions](https://github.com/facebookincubator/velox/pull/2245)
