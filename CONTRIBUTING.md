# Contributing to Presto

Thanks for your interest in Presto.  Our goal is to build a fast, scalable and reliable distributed SQL query engine for running low latency interactive and batch analytic queries against data sources of all sizes ranging from gigabytes to petabytes.

## Getting Started

Presto's [open issues are here](https://github.com/prestodb/presto/issues). Issues that would make a good first pull request for new contributors with the `beginner-task` tag. An easy way to get started helping the project is to *file an issue*. Issues can include bugs, new features, or documentation that looks outdated. For community support, [ask for help in Slack](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM).

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

- Make sure your code follows the [code style guidelines](https://github.com/prestodb/presto#code-style).

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

## Conduct

Please refer to our [Code of Conduct](https://github.com/prestodb/tsc/blob/master/CODE_OF_CONDUCT.md).

## Contributor License Agreement ("CLA")

In order to accept your pull request, we need you to submit a CLA. You only need to do this once, so if you've done this for one repository in the [prestodb](https://github.com/prestodb) organization, you're good to go. If you are submitting a pull request for the first time, the communitybridge-easycla bot will notify you if you haven't signed, and will provide you with a link.  If you are contributing on behalf of a company, you might want to let the person who manages your corporate CLA whitelist know they will be receiving a request from you.

## License

By contributing to Presto, you agree that your contributions will be licensed under the [Apache License Version 2.0 (APLv2)](LICENSE).
