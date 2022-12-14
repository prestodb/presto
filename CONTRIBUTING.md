# Contributing

Welcome! Thank you for your interest in the Velox project. Before starting to
contribute, please, take a moment to review the contribution guidelines
outlined below.

Contributions are not just about code. Contributing code is great, but that’s
probably not the best place to start. There are lots of ways to make valuable
contributions to the project and community.

## Code of Conduct

This project and everyone participating in it is governed by a [Code of Conduct](CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code.

## Community

A good first step to getting involved in the Velox project is to join the
conversations in GitHub Issues and Discussions.

## Bug Reports

Found a bug? Help us by filing an issue on GitHub.

Ensure the bug was not already reported by searching [GitHub Issues](https://github.com/facebookincubator/velox/issues). If you're
unable to find an open issue addressing the problem, open a new one. Be sure to
include a title and clear description, as much relevant information as
possible, and a code sample or an executable test case demonstrating the
expected behavior that is not occurring.

Meta has a [bounty program](https://www.facebook.com/whitehat/) for the safe disclosure 
of security bugs. In those cases, please go through the process outlined on that page 
and do not file a public issue.

## Documentation

Help the community understand how to use the Velox library by proposing
additions to our [docs](https://facebookincubator.github.io/velox/index.html) or pointing 
out outdated or missing pieces.

## Code

This is the process we suggest for code contributions. This process is designed
to reduce the burden on project reviews, impact on other contributors, and to
keep the amount of rework from the contributor to a minimum.

It is good to start with small bug fixes and tiny features to get familiar with
the contributing process and build relationships with the community members.
Look for GitHub issues labeled [good first issue](https://github.com/facebookincubator/velox/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) or consider adding one of the
[missing Presto SQL functions](https://github.com/facebookincubator/velox/issues/2262).

1. Sign the [Contributor License Agreement](https://code.facebook.com/cla) (CLA). This step needs to be completed only once.

1. Review the [`LICENSE`](LICENSE) file. By contributing to Velox, you agree that your
   contributions will be licensed under that LICENSE file. This step needs to be
   completed only once.

1. Start a discussion by creating a Github issue, or asking on Slack (unless the change is trivial).
   * This step helps you identify possible collaborators and reviewers.
   * Does the change align with technical vision and project values?
   * Will the change conflict with another change in progress? If so, work with others to minimize impact.
   * Is this change large? If so, work with others to break into smaller steps.

1. Review our coding style and best practices document in [`CODING_STYLE.md`](CODING_STYLE.md).

   * Implement the change
   * If the change is large, post a preview Github pull request with the title prefixed with [WIP], and share with collaborators.
   * Include tests and documentation as necessary.

1. Create a Github pull request (PR).
   * Give the pull request a clear, brief description: when the pull request is merged, this will be retained in the extended commit message. Check out [How to Write Better Git Commit Messages – A Step-By-Step Guide](https://www.freecodecamp.org/news/how-to-write-better-git-commit-messages/) and [How to Write a Git Commit Message](https://cbea.ms/git-commit/) to learn more about how to write good commit messages.
   * Make sure the pull request passes the tests in CircleCI.
   * If known, request a review from an expert in the area changed. If unknown, ask for help on Slack.

1. Review is performed by one or more reviewers.
   * This normally happens within a few days, but may take longer if the change is large, complex, or if a critical reviewer is unavailable. (feel free to ping the pull request).
   * Address concerns and update the pull request.

1. After pushing the changes, add a comment to the pull-request, mentioning the
   reviewers by name, stating the comments have been addressed. This is the only
   way that a reviewer is notified that you are ready for the code to be reviewed
   again.

1. Go to step 7.

1. Maintainer merges the pull request after final changes are accepted. Due to
   tooling limitations, a Meta employee is required to merge the pull request.

## Presto’s SQL Functions

Here are specific guidelines for contributing Presto SQL functions.

1. Read [How to add a scalar function?](https://facebookincubator.github.io/velox/develop/scalar-functions.html) guide.

2. Use the following template for the PR title: Add xxx Presto function (replace xxx with the function name.)

3. Add documentation for the new function to an .rst file under velox/docs/functions directory.

4. Functions in documentation are listed in alphabetical order. Make sure to
   place the new function so that the order is preserved.

5. Use Presto to check the function semantics. Try different edge cases to see
   whether the function returns null, or throws, or does something else. Make sure
   to replicate Presto semantics exactly.

6. Add tests.

7. Run [Fuzzer](https://facebookincubator.github.io/velox/develop/testing/fuzzer.html)
   to test new the function in isolation as well as in combination with other functions.
   Note: Fuzzer is under active development and the commands below may not work as is.
   Consult the CircleCI configuration in .circleci/config.yml for the up-to-date command
   line arguments.

   ```
   # Test the new function in isolation. Use --only flag to restrict the set of functions
   # and run for 60 seconds or longer.
   velox_expression_fuzzer_test --only <my-new-function-name> --duration_sec 60 --logtostderr=1 --enable_variadic_signatures --velox_fuzzer_enable_complex_types --lazy_vector_generation_ratio 0.2 --velox_fuzzer_enable_column_reuse --velox_fuzzer_enable_expression_reuse

   # Test the new function in combination with other functions. Do not restrict the set
   # of functions and run for 10 minutes (600 seconds) or longer.
   velox_expression_fuzzer_test --duration_sec 600 --logtostderr=1 --enable_variadic_signatures --velox_fuzzer_enable_complex_types --lazy_vector_generation_ratio 0.2 --velox_fuzzer_enable_column_reuse --velox_fuzzer_enable_expression_reuse
   ```

Here are example PRs:

* [Add sha256 Presto function](https://github.com/facebookincubator/velox/pull/1000)
* [Add sin, cos, tan, cosh and tanh Presto functions](https://github.com/facebookincubator/velox/pull/313)
* [Add transform_keys and transform_values Presto functions](https://github.com/facebookincubator/velox/pull/2245)
