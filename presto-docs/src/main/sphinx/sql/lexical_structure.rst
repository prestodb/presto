=================
Lexical Structure
=================

Presto was designed to parse ANSI-compatible SQL according to the SQL
1992 specification. The lexical elements of a SQL statement are
defined by this standard. A SQL command is terminated by a semicolon
character or the end of an input stream.

SQL commands in Presto are a series of tokens that are separated by
whitespace. A token can be a literal, a SQL keyword, or a quoted
identifier.

SQL commands consist of distinct parts: clauses, expressions,
predicates, queries, and statements to form a command which returns a
result set or a response.

------------------------
Keywords and Identifiers
------------------------

Keywords are tokens which are specified in the SQL specification.
Tokens such as SELECT, INSERT, UPDATE, and DELETE are assigned meaning
according to the specification.

Identifiers are names of catalogs, schemas, tables, columns, and other
database objects.