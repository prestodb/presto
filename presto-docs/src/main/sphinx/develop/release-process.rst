=================
Release Process
=================

Overview
--------

Presto releases are managed by volunteer committers. Releases occur approximately every 2 months, with extended testing periods to ensure stability.

Release Cadence
---------------

Releases target a 2-month cycle. Actual timing depends on:

* Release shepherd availability
* Testing feedback
* Critical issues requiring delay
* Contributing organization resources

Schedules adjust based on volunteer availability.

Release Quality Model
---------------------

**Note:** Trunk is not stable. Do not use master branch in production.

**Extended Release Candidate Period**
   * 2-4 week RC period for testing
   * Issues fixed before final release
   * Fixes verified in existing RC (no new RCs)

**Community Testing**
   * Organizations test RCs in their environments
   * Weekly edge releases from master for early testing
   * Join #releases in `Presto Slack <https://communityinviter.com/apps/prestodb/prestodb>`_ to participate

**Automated Testing**
   * Unit tests on all commits and RCs
   * Product tests with full cluster deployment
   * Shared connector tests via `AbstractTestQueries <https://github.com/prestodb/presto/blob/master/presto-tests/src/main/java/com/facebook/presto/tests/AbstractTestQueries.java>`_
   * CI for basic stability checks

**Testing Contributions Needed**
   * Additional `product test scenarios <https://github.com/prestodb/presto/tree/master/presto-product-tests>`_
   * Performance regression testing (shadow traffic)
   * Test coverage for new features

Version Numbering
-----------------

* Format: ``0.XXX`` (e.g., 0.293, 0.294)
* Major version fixed at 0
* Minor version increments each release
* Patch releases (e.g., 0.293.1) for critical fixes only

Not semantic versioning.

Release Types
-------------

**Regular Releases**
   * Every ~2 months
   * New features, improvements, bug fixes
   * Extended RC testing period

**Patch Releases**
   * Critical issues only:
     
     - Security vulnerabilities (upon request - default: upgrade)
     - Data correctness
     - Performance regressions
     - Stability issues
   
   * Based on previous stable release
   * Minimal changes

**Release Candidates**
   * 2-4 week testing period
   * One RC per release
   * Fixes verified in existing RC

**Edge Releases**
   * Weekly from master
   * Early access to features
   * Testing only - not for production

Release Shepherd Responsibilities
----------------------------------

Requirements:

* Must be a committer
* Must understand codebase for judgment calls and rewrites

Responsibilities:

* Complete release process
* Ensure release notes follow guidelines
* Fix release note issues
* Cut and deploy release
* Coordinate with community
* Make go/no-go decisions

See `Release Shepherding <https://github.com/prestodb/presto/wiki/Release-Shepherding>`_ for schedule and details.

Contributing to Release Quality
-------------------------------

**Testing**
   * Run RCs in test environments
   * Report issues with reproduction steps
   * Verify cherry-picked fixes

**Test Development**
   * Add product tests for uncovered scenarios
   * Unit tests for bug fixes
   * Performance benchmarks (`pbench <https://github.com/prestodb/pbench>`_)

**Documentation**
   * Document behavior changes
   * Write clear release notes
   * Note compatibility issues

**Code Review**
   * Review PRs for correctness
   * Identify compatibility issues
   * Suggest test coverage

Backward Compatibility Guidelines
----------------------------------

**Must Maintain Compatibility:**

* **Client Libraries**: Evolve slowly. New server features must work with older clients.
* **SQL Syntax**: Keep stable. Deprecate with warnings before removal.
* **SPI**: Stable for connectors/plugins. Use ``@Deprecated`` for at least five releases before removal.
  When adding new SPI methods, provide reasonable defaults to minimize connector updates.
  Exception: Undocumented/unused SPI aspects.
* **Configuration**: Session and config properties need deprecation paths. Provide aliases for renames.

**Can Change:**

* Internal APIs (not SPI)
* Performance characteristics
* Query plans
* Default config values (document changes)

**Developer Requirements:**

* Document breaking changes in release notes
* Provide migration paths
* Test with older clients
* Revert inadvertent breaking changes to client protocol, SQL, SPI, or config

Release Communication
---------------------

* `Presto Slack <https://communityinviter.com/apps/prestodb/prestodb>`_ #releases channel
* `GitHub Releases <https://github.com/prestodb/presto/releases>`_
* `Mailing List <https://lists.prestodb.io/g/presto-dev>`_
* Release notes in docs

Best Practices for Developers
------------------------------

* Test changes against RCs
* Monitor #releases channel during release cycles
* Fix release blockers promptly
* Document breaking changes in release notes
* Avoid risky merges near release cuts
* Test your areas during RC phase

Getting Involved
----------------

* Join #releases in `Presto Slack <https://communityinviter.com/apps/prestodb/prestodb>`_
* Test release candidates
* Volunteer as a `release shepherd <https://github.com/prestodb/presto/wiki/Release-Shepherding>`_  (committers only)
* Contribute tests
* Share production experiences