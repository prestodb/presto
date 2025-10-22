===============
Release Process
===============

Overview
========

Presto releases are managed by volunteer committers. Releases occur approximately every 2 months, with extended testing periods to ensure stability.

Release Cadence
===============

Releases target a 2-month cycle. Actual timing depends on:

* Release shepherd availability
* Testing feedback
* Critical issues requiring delay
* Contributing organization resources

Schedules adjust based on volunteer availability.

Release Quality Model
=====================

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
   * Shared connector tests with `AbstractTestQueries <https://github.com/prestodb/presto/blob/master/presto-tests/src/main/java/com/facebook/presto/tests/AbstractTestQueries.java>`_
   * CI for basic stability checks

**Testing Contributions Needed**
   * Additional `product test scenarios <https://github.com/prestodb/presto/tree/master/presto-product-tests>`_
   * Performance regression testing (shadow traffic)
   * Test coverage for new features

Version Numbering
=================

* Format: ``0.XXX`` (for example, 0.293, 0.294)
* Major version fixed at 0
* Minor version increments each release
* Patch releases (for example, 0.293.1) for critical fixes only

Not semantic versioning.

Release Types
=============

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
=================================

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
===============================

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
=================================

**Must Maintain Compatibility:**

* **Client Libraries**: Evolve slowly. New server features must work with older clients.
* **SQL Syntax**: Keep stable. Deprecate with warnings before removal.
* **SPI**: Stable for connectors/plugins. Use ``@Deprecated`` for at least two releases before removal.
  When adding new SPI methods, provide reasonable defaults to minimize connector updates.
  Documented SPI interfaces must remain stable even without public implementations.
  Exception: Undocumented AND unused SPI aspects.
* **Configuration**: Session and config properties need deprecation paths. Provide aliases for renames.

**Can Change:**

* Internal APIs (not SPI)
* Performance characteristics
* Query plans
* Default config values (document changes)

**Developer Requirements:**

* Document breaking changes in release notes
* Provide migration paths
* Revert inadvertent breaking changes to client protocol, SQL, SPI, or config

Revert Guidelines
=================

When to Revert
^^^^^^^^^^^^^^

Data Correctness Issues or Critical Bugs
----------------------------------------

Any change that introduces data correctness issues, major crashes, or severe stability problems
must be reverted immediately if a fix is not quick, especially near the RC finalization window.

**Must revert**:

- Wrong query results, data corruption, frequent crashes
- Memory leaks or resource exhaustion in common code paths

**Should revert**:

- Performance regressions of more than 50% in common queries

Backwards Incompatible Client Changes
-------------------------------------

Client libraries evolve slowly and many users cannot easily upgrade clients. Breaking changes to
the client protocol, SQL syntax, or session/config properties without proper migration paths must
be reverted if they cannot be fixed quickly, particularly near RC finalization:

**Must revert**:

- Breaking client-server protocol compatibility
- Removing SQL syntax without deprecation warnings

**Should revert**:

- Changing session/config property behavior without aliases

Backwards Incompatible SPI Changes Without Migration Path
---------------------------------------------------------

If a backwards incompatible change to the SPI is discovered that lacks the required migration path
(for example, no deprecation period, no reasonable defaults for new methods), the change should be reverted
if a proper migration path cannot be added quickly, especially near RC finalization. Use these criteria:

**Must revert**:

- Breaking documented SPI interfaces or core connectors (Hive, Iceberg, Delta, Kafka)
- Breaking maintained connectors with active usage in the repository

**Should revert**:

- Breaking experimental or rarely-used connectors (weigh maintenance burden)

Consider both documented interfaces and public implementations in the Presto repository.
Create a GitHub issue marked as "release blocker" to alert the release shepherd.

When NOT to Revert
^^^^^^^^^^^^^^^^^^

If the fix is simpler than the revert and can be completed quickly (especially before RC finalization), prefer fixing forward.

**Fix forward**:

- Typos
- Logging issues
- Minor UI problems
- Test failures that don't affect production code
- Documentation errors or missing documentation

Performance Trade-offs
----------------------

Performance changes with mixed impact require case-by-case evaluation based on community feedback:

- **Evaluate carefully**: What's rare for one user may be critical for another
- **Consider configuration**: Can the optimization be made optional by using session properties?
- **Gather data**: Solicit feedback from multiple organizations during RC testing

If multiple users report significant regressions, consider reverting or adding a feature flag.
Always document performance changes and workarounds in release notes.

Proprietary or Hidden Infrastructure Dependencies
-------------------------------------------------

Changes cannot be reverted based on impacts to proprietary infrastructure, private forks, or
non-public connectors or plugins. All revert decisions must be justifiable using only publicly
available code, documentation, and usage patterns visible in the open source project.

Feature Additions With Minor Issues
-----------------------------------

New features that don't affect existing functionality should be fixed.
Consider adding feature flags if stability is a concern.

How to Revert
-------------

- Create a GitHub issue that describes the problem and label it "release blocker" immediately
- Raise a PR to revert the problematic change and link to the issue

Release Communication
=====================

* `Presto Slack <https://communityinviter.com/apps/prestodb/prestodb>`_ #releases channel
* `GitHub Releases <https://github.com/prestodb/presto/releases>`_
* `Mailing List <https://lists.prestodb.io/g/presto-dev>`_
* `Release notes in docs <https://prestodb.io/docs/current/release/release.html>`_

Best Practices for Developers
=============================

* Avoid risky merges near release cuts
* Create as many automated tests as possible, and for large changes, consider product tests and manual testing
* Always consider how new features are enabled, whether they're enabled by default, and if not opt-in through SPI or SQL, gate them with a session property
* Document breaking changes in release notes
* Monitor #releases channel during release cycles
* Fix release blockers promptly

Getting Involved
================

* Join #releases in `Presto Slack <https://communityinviter.com/apps/prestodb/prestodb>`_
* Test release candidates
* Volunteer as a `release shepherd <https://github.com/prestodb/presto/wiki/Release-Shepherding>`_  (committers only)
* Contribute tests
* Share production experiences