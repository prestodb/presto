===============
Version Support
===============

Overview
--------

Presto is maintained by volunteers. This document describes which versions receive support and what level of support to expect.

Support Philosophy
------------------

* Data correctness issues are taken extremely seriously and typically fixed quickly
* Runtime bugs and security vulnerabilities are prioritized and addressed promptly
* Support depends on volunteer availability - no formal SLAs
* Users are encouraged to contribute fixes for issues affecting them

.. _current-version-support:

Current Version Support
-----------------------

**Latest Release**
   * Primary focus for bug fixes
   * Recommended for new deployments after testing

**Past 4 Releases (N-1 through N-4)**
   * Critical fixes only when:
     
     - Data correctness issues are found
     - Volunteers are available to backport
   
   * Patch releases for severe issues only
   * Support decreases with age

**Older Releases (N-5 and earlier)**
   * Not supported
   * Exceptions only when:
     
     - Volunteer provides the backport
     - Fix applies cleanly
     - Testing is available
   
   * Upgrade required

**Trunk/Master Branch**
   * Development branch
   * **Never use in production**
   * Contains experimental features and bugs
   * For testing upcoming changes only

**Edge Releases**
   * Weekly builds from master
   * **Never use in production**
   * **Not supported** - no fixes provided
   * For testing upcoming features

Support Lifecycle
-----------------

Timeframes are approximate and depend on volunteer availability.

A typical release follows this lifecycle:

1. **Release Candidates** (2-4 weeks)
   
   - One RC version per release
   - Active bug fixing with fixes verified in the existing RC
   - High community engagement

2. **Current Release** (approximately 2 months)
   
   - Primary focus for bug fixes
   - Active monitoring for issues
   - Most community attention

3. **Supported Releases** (N-1 through N-4, approximately 8 months)
   
   - Critical fixes only
   - Progressively reduced community focus
   - Patch releases for severe issues become less likely with age

4. **Archived** (N-5 and older)
   
   - No active support
   - Users strongly encouraged to upgrade
   - See :ref:`current-version-support` for details

Types of Support
----------------

**Bug Fixes**
   Highest priority (typically fixed very quickly):
   
   * Data correctness issues - taken extremely seriously
   
   High priority:
   
   * Runtime bugs and crashes
   * Severe performance regressions
   
   Lower priority:
   
   * Minor performance issues
   * UI/cosmetic problems
   * Feature enhancements

**Security Vulnerabilities**
   * Upgrade to latest release (default recommendation)
   * Patches for N-1 through N-4 available upon request
   * Backport availability depends on volunteers and severity
   * Plan to upgrade rather than rely on backports

**Documentation**
   * Release notes and full documentation for all versions remain available
   * Migration guides for major changes
   * Community-contributed upgrade experiences

Getting Support
---------------

**Community Channels**

* `Presto Slack <https://communityinviter.com/apps/prestodb/prestodb>`_ - Real-time community discussion
* `GitHub Issues <https://github.com/prestodb/presto/issues>`_ - Bug reports and feature requests
* `Mailing List <https://lists.prestodb.io/g/presto-dev>`_ - Development discussions

**Self-Support Resources**

* Release notes and documentation
* Community Slack search history
* GitHub issues and pull requests
* Stack Overflow questions tagged 'presto'

Recommendations for Production Use
----------------------------------

**Version Selection**

1. **For new deployments**: Use the latest stable release after thorough testing
2. **For existing deployments**: Stay within 4 versions of the latest release
3. **For conservative environments**: Wait for at least one patch release (if any) before upgrading
4. **Never use trunk/master or edge** in production

**Upgrade Strategy**

* Plan regular upgrades (every 2-4 months)
* Test thoroughly in staging environments
* Monitor community channels for known issues
* Maintain ability to rollback if needed
* Consider skipping releases if stable (but don't fall too far behind)

**Risk Mitigation**

* Maintain test environments matching production
* Participate in release candidate testing
* Monitor community discussions for your version
* Contribute test cases for critical workflows

Contributing to Support
-----------------------

Ways to contribute:

**Report Issues**
   * File detailed bug reports with reproduction steps on `GitHub Issues <https://github.com/prestodb/presto/issues>`_
   * Test fixes and provide feedback
   * Share workarounds with the community

**Contribute Fixes**
   * Submit `pull requests <https://github.com/prestodb/presto/pulls>`_ for bugs affecting you
   * Help review and test others' fixes
   * Backport critical fixes to versions you use

**Share Knowledge**
   * Document upgrade experiences
   * Answer questions in `Presto Slack <https://communityinviter.com/apps/prestodb/prestodb>`_
   * Write blog posts about solutions
   * Contribute to `documentation <https://github.com/prestodb/presto/tree/master/presto-docs>`_

**Sponsor Development**
   * Allocate engineering resources to the project
   * Fund specific feature development
   * Support maintainers and release shepherds

Special Considerations
----------------------

**Long-Term Support (LTS)**
   * Not available
   * Volunteer model incompatible with LTS commitments

**End-of-Life Announcements**
   * No formal EOL process
   * Versions become unsupported as community moves forward
   * Check release announcements for migration guidance

**Compatibility**
   * Breaking changes documented in release notes
   * Migration guides provided for major changes
   * Test when upgrading across multiple versions

Support Expectations
--------------------

**Available:**

* Typically quick response to data correctness and runtime bugs
* Priority focus on critical issues
* Active community troubleshooting help
* Transparency about known issues
* Documentation for old versions

**Not Available:**

* Guaranteed response times
* Fixes for all issues
* Support for old versions
* Feature backports
* 24/7 support

Summary
-------

Running Presto in production requires:

* Regular upgrades (every 2-4 months)
* Thorough testing before deploying
* Understanding that support is volunteer-based
* Contributing fixes for issues you encounter