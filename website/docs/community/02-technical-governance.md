---
id: technical-governance
title: Technical Governance
---

This document describes the Velox technical governance structure, how it
operates, and the decision-making process behind the project.

## Summary

The Velox community is composed of a set of highly-skilled individuals with the
shared purpose of achieving the project mission. The project adopts a
hierarchical technical governance structure, solely based on individuals and
their merit in the community. Velox’s governing structure is composed of:

* A community of **contributors** who file issues, make pull requests, ask and
  answer questions, and overall contribute to the project.
* A small set of **component maintainers** who are responsible for supporting,
  enhancing, and maintaining the health of each component of the Velox library.
* They are supported by a **project leadership council (PLC)**, responsible for
  long-term direction and strategy of the project.
  * The leadership council has a **chair** who is responsible for settling
    eventual disputes.

All maintainers are expected to have a strong bias towards [Velox’s design
philosophy.](./design-philosophy)

Beyond the maintainers, the community is encouraged to contribute, file issues,
make proposals, review pull requests, help answer questions, publish blog
posts, share best practices, and be present in the community. Given
contributions, technical skills, and willingness to invest, anyone can be
accepted as a maintainer and granted responsibility over parts of the codebase.

Technical governance is merit-based and strictly separated from business
governance. Separating technical from business governance ensures that the only
way for an individual to join the technical governance body is through
continued contributions and demonstration of deep technical skills in the
domain, great judgment, and alignment with project principles and philosophy. 

Furthermore, membership in the technical governance body is for **individuals**,
not companies. That is, there are no seats reserved for specific companies, and
membership is associated with the person rather than their affiliation. 

## Technical Governance Structure

### Component Maintainers

Due to its large breadth and complexity, Velox’s codebase is organized into a
few logical components. Note that these components are logical and do not
necessarily reflect a strict physical separation of files and directories,
although in some cases they do.

Each Velox component will have its own set of maintainers, composed of a set of
highly-skilled individuals who have continually demonstrated strong technical
expertise, engagement, great technical judgment, and ownership over that
component. They are responsible for:

* Review and approve pull requests.
* Respond to bug reports and questions about the component.
* Participate in design discussions about the component.
* Maintain and improve codebase health, fix broken and flaky tests, fuzzer
  found bugs, and other CI issues.
* Improve test and performance coverage.
* Enhance component design and APIs.
* Produce and keep documentation up-to-date. 

Component maintainers have independence over local decisions affecting their
component, given alignment with project principles. They also have the right to
dispute decisions made by other component maintainers - especially if it
affects them. When disputes are made, the component maintainer group should
provide a reasonable explanation of the dispute, the relevant arguments, and
the resolution. 

In the exceptional cases where component maintainers cannot come to a
conclusion themselves, they may escalate it to the PLC for review. The
escalations are resolved by the PLC in accordance with their rules and
procedures.

### Project Leadership Council - PLC

The project leadership council is a small group of highly-skilled individuals
who have demonstrated deep and broad expertise in the domain of query execution
and data management, and are actively engaged in the community. They have a
deep understanding of the Velox codebase and all of its integrations, and are
responsible for the project leadership, long-term direction, and strategy. PLC
members are also accountable for upholding the project principles and
philosophy. They are responsible for:

* Long-term directional leadership of the project.
* Dispute local decisions when in conflict with project values, principles, or
  direction.
* Negotiating and resolving contentious issues, upkeeping the project’s best
  interest.
* Receiving broad requests for changes from project stakeholders and evaluating
  them.
  * Localized component-level requests are handled by component maintainers.
* Nominate and confirm component maintainers and other PLC members.

The project leadership council as a group has the power to dispute decisions
made at the component maintainer level, and to resolve disputes using their
best judgment. PLC members should publicly articulate their decision-making,
and give a clear reasoning for their decisions, vetoes, and dispute resolution.

### Project Leadership Council Chair

To ensure the PLC can always reach a decision, they have an assigned and
publicly declared chair amongst them. The chair is responsible for settling
disputes in exceptional cases where the PLC could not come to a consensus. The
PLC chair should publicly articulate their decision-making, and give a clear
reasoning for their decisions.

## Voting and Nominations

### The Principles

* Membership in maintainer groups is given to **individuals** on **merit
  basis** after they demonstrated strong expertise of the component through
  contributions, code reviews, design discussions, and community engagement.
* For membership in the maintainer group, the individual has to demonstrate
  strong and continued alignment with the project principles, philosophy, and
  technical direction.
* Component maintainers must demonstrate exceptional code review skills,
  proactiveness and thoroughness, in addition to great technical judgment.
* Component maintainers must demonstrate a deep care for engineering
  excellence, testability, API modularity, documentation, and more, keeping a
  high bar for Velox’s codebase. 
* In order to be considered, individuals must have demonstrated contributions
  for a sustained period of time, typically in the order of 6 months or longer.
  * This means maintainer group membership is *lagging*, not *leading*.
* No term limits for component maintainers, PLC members, or PLC chair.
* Light criteria of moving component maintenance or PLC members to *emeritus*
  status if they don’t actively participate over long periods of time. 
* The membership is for an individual, not a company.

### The Process for Nomination

* Anyone in the community can nominate a person to become a component
  maintainer.
* Periodically, the project leadership council will go through the nominations,
  do light filtering around spam or desk-rejection, and draw up a list of
  potential nominees.
* PLC members may request for more information on the nominee. The information
  should consist of:
  * The nominees depth and breadth of code, review, and design contributions on
    the component.
  * Testimonials (positive and negative) of the nominee’s interactions with the
    maintainers, users, and the community.
  * General testimonials of support from the maintainers.
* The PLC then evaluates all information and makes a final decision through
  majority vote to confirm or decline the nomination. 

### Nominating Core Maintainers

* Any PLC member or component maintainer can nominate someone to become a PLC
  member.
* PLC members are responsible for evaluating the nomination.
* The PLC may requests for additional information around the strength of the
  candidate to be a PLC member:
  * Evidence that the individual is in fact helping drive the overall project
    direction.
  * List the individual’s contributions to the project.
  * Letters of support from other component maintainers or PLC members.
  * General letters of support from stakeholders within the Velox community.
  * Any new relevant information that is befitting for the candidacy.
* The PLC evaluates all information and makes a final decision through
  unanimous vote to confirm or decline the nomination, with a clear public
  articulation of their reasoning behind the decision.

### The Process for Removal

* Similar to the process for nomination, anyone in the community can nominate a
  person to be removed from a component maintainer position or from the PLC.
  * A person can also self-nominate to be removed.
  * Individuals may also be moved to emeritus status after a year of continued
    inactivity in the community. 
* The PLC (excluding persons with conflict of interest) will request and
  evaluate the provided evidence, such as Code of Conduct violations or
  activity outside the scope of the project that may conflict with the
  project’s values.
* The PLC then evaluates all information and makes a final decision through
  unanimous vote to confirm or decline the removal, with a clear public
  articulation of their reasoning behind the decision.

### PLC Chair Substitution

* A super-majority of PLC members (75%) can choose to remove the PLC chair.
* After a removal of the PLC chair or in unforeseen circumstances (such as
  permanent unavailability of the current PLC chair), the project leadership
  council will follow a ranked-choice voting method to elect a new chair.

## Decision Making

### Uncontroversial Changes

Primary work happens through Issues, Discussions, and pull requests on GitHub.
Component maintainers ultimately approve pull requests allowing them to be
merged without further process.

Notifying relevant experts about an issue or a pull request is important.
Reviews from experts in the given interest area are strongly preferred,
especially on pull request approvals. Failure to do so might end up with the
change being reverted by the relevant expert. Contributors are also encouraged
to find local experts using git log/blame. 

The component maintainers along with pull request authors themselves are
responsible for finding and notifying experts to help review a particular
change.

### Controversial Decision Process

Substantial changes in a given interest area require a GitHub Issue to be
opened for discussion. Components maintainers are responsible for participating
in the discussion, reviewing, providing feedback, and eventually approving the
changes. If disputes are made over the change, component maintainers are
responsible for settling the issue, or ultimately escalating to the PLC in case
they cannot reach consensus. 

### Re-Scope Project Components

The PLC is responsible for taking decisions on adding, removing, and re-scoping
project components. They invite proposals from members in the community
(including themselves) for such changes, and should publicly articulate the
reasoning behind the decision.

### Changes to Governance Mechanics

Any changes to this document or the overall technical governance mechanics of
the project needs to be voted and unanimously approved by PLC members.

## General Project Policies

Velox participants acknowledge that the copyright in all new contributions will
be retained by the copyright holder as independent works of authorship and that
no contributor or copyright holder will be required to assign copyrights to the
project. Except as described below, all code contributions to the project must
be made using the Apache 2.0 License available here:

[https://www.apache.org/licenses/LICENSE-2.0
](https://www.apache.org/licenses/LICENSE-2.0) (the "Project License"). 

All outbound code will be made available under the Project License. The
maintainers may approve the use of an alternative open license or licenses for
inbound or outbound contributions on an exception basis.

## FAQ

**Q: I would like to help and get engaged with the community. Where do I
start?**
The Velox [contributing
guide](https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md)
provides guidelines on how new community members can get involved in the
project. 

**Q: Is it possible for an external contributor to become a maintainer and be
granted responsibilities over parts of the codebase?**
This is absolutely possible. The first step is to start contributing to the
existing project area and supporting its health and success. In addition to
this, you can make a proposal through a GitHub issue for new functionality or
changes to improve the project area.

**Q: What if I am a company looking to use Velox internally for development, can
I be granted or purchase a maintainer or PLC member status?**
No, the Velox project is strictly driven by highly-skilled technical
individuals, and membership in these groups is based on merit in the community.

**Q: How do I contribute code to the project?**
If the change is relatively minor, a pull request on GitHub can be opened up
immediately for review by the project maintainers. For larger changes, please
open an Issue to make a proposal to discuss prior. Please also see the 
[Velox Contributor Guide](https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md)
for contribution guidelines.

**Q: Can I become a committer on the project?**
Unfortunately, the commit process to Velox involves an interaction with Meta’s
infrastructure that currently can only be triggered by Meta employees.
Maintainers, however, have the authority to approve PRs, which then get merged
without further process by a Meta-employee-based oncall rotation. We intend to
improve our tooling to relax this restriction in the future. 
