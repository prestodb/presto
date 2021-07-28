# Contributing to Presto

Thanks for your interest in Presto.  Our goal is to build a fast, scalable and reliable distributed SQL query engine for running low latency interactive and batch analytic queries against data sources of all sizes ranging from gigabytes to petabytes.

## Getting Started

Presto's [open issues are here](https://github.com/prestodb/presto/issues). Issues that would make a good first pull request for new contributors with the `beginner-task` tag. An easy way to get started helping the project is to *file an issue*. Issues can include bugs, new features, or documentation that looks outdated. For community support, [ask for help in Slack](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM).

## Contributions

Presto welcomes contributions from everyone.

Contributions to Presto should be made in the form of GitHub pull request submissions and reviews. 

Each pull request submission will be reviewed by a contributor or [committer](https://github.com/prestodb/presto/wiki/committers) 
in the project. Only committers may merge a pull request. Large contributions should have an associated Github issue.

Pull request reviews are encouraged for anyone in the community who would like to contribute to Presto, and are
expected from contributors and committers in at least equal proportion to their code contributions.

## Pull Request Checklist

- Branch from the master branch and, if needed, rebase to the current master
  branch before submitting your pull request. If it doesn't merge cleanly with
  master you may be asked to rebase your changes.

- If your pull request does not have a reviewer
  assigned to it after a few days, [ask for a review in the #dev channel in Slack](https://join.slack.com/t/prestodb/shared_invite/enQtNTQ3NjU2MTYyNDA2LTYyOTg3MzUyMWE1YTI3Njc5YjgxZjNiYTgxODAzYjI5YWMwYWE0MTZjYWFhNGMwNjczYjI3N2JhM2ExMGJlMWM).

- Tests are expected for all bug fixes and new features.

- Make sure your code follows the [code style guidelines](https://github.com/prestodb/presto#code-style).

- Make sure you follow the [review and commit guidelines](https://github.com/prestodb/presto/wiki/Review-and-Commit-guidelines), in particular:

    - Ensure that each commit is correct independently (i.e., each commit should compile and pass tests).
      When possible, the size of the commit should be reduced for ease of review, while still ensuring
      that it is independently correct.

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
