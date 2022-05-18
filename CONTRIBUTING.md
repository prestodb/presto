# Contributing to Velox

Thank you for your interest in contributing to **Velox**! Before you begin
writing code, it is important that you share your intention to contribute with
the team, based on the type of contribution:

1. You want to propose a new feature and implement it:
  * Post about your intended feature in an issue, and we shall discuss the
    design and implementation. Once we agree that the plan looks good, go ahead
    and implement it.
2. You want to implement a feature or bug-fix for an outstanding issue.
  * Search for your issue in [the Velox issue list.](https://github.com/facebookincubator/velox/issues)
  * Pick an issue and comment that you'd like to work on the feature or
    bug-fix.
  * If you need more context on a particular issue, please ask and we shall
    provide.

The main communication channel with the Velox community is [the Velox-OSS Slack
workspace](http://velox-oss.slack.com). Please don't hesitate in reaching out.
While doing so, in order to ensure that all contributions are timely reviewed,
we recommend authors to start a discussion with the team and make code review
arrangements.

Once you implement and test your feature or bug-fix, please submit a Pull
Request to <https://github.com/facebookincubator/velox>, tagging the reviewers
you previously identified.

We also encourage authors to proactively communicate during code review; please
provide an ETA on when review comments will be addressed (if applicable), and
explicitly notify reviewers when the PR is ready for another round of reviews.

## Pull Requests

We actively welcome your pull requests.

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. If you haven't already, complete the Contributor License Agreement ("CLA").

## Coding Style and Best Practices

Before contributing to Velox, please review our coding style and best practices
document in [`CODING_STYLE.md`](CODING_STYLE.md).

## Code Formatting, Headers, and Licenses

Our Makefile contains targets to help highlighting and fixing format, header or
license issues. These targets are shortcuts for calling `./scripts/check.py`.

Use `make header-fix` to apply our open source license headers to new files.
Use `make format-check` to highlight formatting issues using clang-format.

Formatting issues found on the changed lines in the current commit can be
displayed using `make format-check`.  These issues can be fixed by using `make
format-fix`. This command will apply formatting changes to modified lines in
the current commit.

Header issues found on the changed files in the current commit can be displayed
using `make header-check`. These issues can be fixed by using `make header-fix`.
This will apply license header updates to files in the current commit.

An entire directory tree of files can be formatted and have license headers
added using the `tree` variant of the format.sh commands:
```
    ./scripts/check.py format tree
    ./scripts/check.py format tree --fix

    ./scripts/check.py header tree
    ./scripts/check.py header tree --fix
```

All the available formatting commands can be displayed by using
`./scripts/check.py help`.

## Continuous Integration and CircleCI

Velox uses CircleCI as the continuous integration system, so please ensure your
PR does not break any of these workflows. CircleCi runs `make format-check`,
`make header-check` as part of our continuous integration. Pull requests should
pass format-check and header-check without errors before being accepted.

More details can be found at [.circleci/REAME.md](.circleci)

## Contributor License Agreement ("CLA")

In order to accept your pull request, we need you to submit a CLA. You only need
to do this once to work on any of Facebook's open source projects.

Complete your CLA here: <https://code.facebook.com/cla>

## Issues

We use GitHub issues to track public bugs. Please ensure your description is
clear and has sufficient instructions to be able to reproduce the issue.

Facebook has a [bounty program](https://www.facebook.com/whitehat/) for the safe
disclosure of security bugs. In those cases, please go through the process
outlined on that page and do not file a public issue.

## Code of Conduct

The code of conduct is described in [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).

## License

By contributing to Velox, you agree that your contributions will be licensed
under the LICENSE file in the root directory of this source tree.
