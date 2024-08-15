# Presto Function Guidelines

Presto includes a large library of built-in SQL functions such as AVG and CONCAT that are available in every deployment.

Presto also supports plugins that provide extension functions that are not available by default but can be installed in particular deployments. Examples include the [I18nMyanmarFunctions](https://github.com/prestodb/presto/blob/master/presto-i18n-functions/src/main/java/com/facebook/presto/i18n/functions/I18nMyanmarFunctions.java) for working with Zawgyi encoded text. If Burmese text is present in your data store, these can be important, but more Presto installations don't need this than do.

Finally, Presto allows users to write user defined functions (UDFs) in either Java or SQL that are not stored in this GitHub repository. This is done through the [Plugin SPI](https://prestodb.io/docs/current/develop/functions.html).

This document lays out some guidelines for which category is right for which functions. 

Functions defined in [ANSI SQL](https://jakewheat.github.io/sql-overview/) should be built-in functions in this repository.

Non-standard functions that are commonly available in multiple other SQL dialects such as MySQL and Postgres can also be written as built-in functions in this library.

Functions that are provided in order to mirror the syntax and semantics of particular other SQL dialects (MySQL, Postgres, Oracle, etc.) should be extension functions so they can be turned on by users who need compatibility with that dialect while everyone else can safely ignore them. This helps avoid conflicts where two existing dialects are incompatible. The [Teradata plugin](https://github.com/prestodb/presto/tree/master/presto-teradata-functions/src/main/java/com/facebook/presto/teradata) is a good example of this.

Functions that alias or closely resemble existing built-in functions should not be built-in functions and usually should not be extension functions either. For example, because Presto already has an AVG function, it should not also have a MEAN or an AVERAGE function, even if the semantics or arguments are slightly different. However, extension functions whose purpose is to mirror other SQL dialects may occasionally alias functions. For instance, the Teradata plugin provides an index() function that is an alias for Presto's built-in strpos() function. This is purely for compatibility with existing Teradata SQL and should be invisible to users who have not deliberately chosen to install the Teradata plugin.

Built-in functions should be broadly applicable to many users in different areas. For example,
functions that work with pre-decimalization English currency — pounds, shillings, pence, etc. —  would be unlikely to be needed by many users, and thus should be extension functions or UDFs.

Built-in functions should be purely computational. That is, they do not make network connections or perform I/O outside the database. (Date/time functions like NOW() are an exception.) For example, a function that talks to a remote server to check the current temperature or a stock price should not be a built-in function. This should be a user defined function maintained outside the Presto GitHub repository.

Built-in and extension functions should be able to be maintained by Presto committers without support from the original contributors. That is, they should not require specialized or expert knowledge beyond what Presto maintenance already requires. For instance, most cryptographic functions should not be included in this repository because it is not reasonable to expect that Presto committers are also cryptography experts.

Built-in functions that extend Presto into new domains should have a detailed plan and ideally an implementation for a library of functions that cover the domain. For instance, a function that only added US dollars would not be accepted. However, a general library for working with currency data along the lines of [Joda-Money](https://www.joda.org/joda-money/) could be considered. It can be helpful to prototype complex functionality like this in UDFs before writing an RFC and submitting it to the Presto project.

Built-in and extension functions bundled with Presto must be freely available without fee and compatible with the Apache License Version 2.0. Functions using algorithms that are in any way restricted by copyright, patent, trademark, field of use, or other reasons will not be included in this repository. For instance, a patented video CODEC cannot be used. Functions that include trademarked names should be UDFs outside this repository.

