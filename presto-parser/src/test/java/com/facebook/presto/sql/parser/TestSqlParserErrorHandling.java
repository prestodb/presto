/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.parser;

import com.google.common.base.Joiner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSqlParserErrorHandling
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @DataProvider(name = "expressions")
    public Object[][] getExpressions()
    {
        return new Object[][] {
                {"", "line 1:1: no viable alternative at input '<EOF>'"},
                {"1 + 1 x", "line 1:7: extraneous input 'x' expecting {<EOF>, '.', '[', 'AND', 'AT', 'BETWEEN', 'IN', 'IS', 'LIKE', 'NOT', 'OR', '=', NEQ, '<', '<=', '>', '>=', '+', '-', '*', '/', '%', '||'}"},
                };
    }

    @DataProvider(name = "statements")
    public Object[][] getStatements()
    {
        return new Object[][] {
                {"",
                 "line 1:1: no viable alternative at input '<EOF>'"},
                {"@select",
                 "line 1:1: extraneous input '@' expecting {'(', 'ALTER', 'CALL', 'COMMIT', 'CREATE', 'DEALLOCATE', 'DELETE', 'DESC', 'DESCRIBE', 'DROP', 'EXECUTE', 'EXPLAIN', " +
                         "'GRANT', 'INSERT', 'PREPARE', 'RESET', 'REVOKE', 'ROLLBACK', 'SELECT', 'SET', 'SHOW', 'START', 'TABLE', 'USE', 'VALUES', 'WITH'}"},
                {"select * from foo where @what",
                 "line 1:25: extraneous input '@' expecting {'(', '?', 'ADD', 'ALL', 'ANALYZE', 'ANY', 'ARRAY', 'ASC', 'AT', 'BERNOULLI', 'CALL', 'CASCADE', 'CASE', 'CAST', " +
                         "'CATALOGS', 'COALESCE', 'COLUMN', 'COLUMNS', 'COMMENT', 'COMMIT', 'COMMITTED', 'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', " +
                         "'DATA', 'DATE', 'DAY', 'DESC', 'DISTRIBUTED', 'EXCLUDING', 'EXISTS', 'EXPLAIN', 'EXTRACT', 'FALSE', 'FILTER', 'FIRST', 'FOLLOWING', 'FORMAT', 'FUNCTIONS', " +
                         "'GRANT', 'GRANTS', 'GRAPHVIZ', 'GROUPING', 'HOUR', 'IF', 'INCLUDING', 'INPUT', 'INTEGER', 'INTERVAL', 'ISOLATION', 'LAST', 'LATERAL', 'LEVEL', 'LIMIT', " +
                         "'LOCALTIME', 'LOCALTIMESTAMP', 'LOGICAL', 'MAP', 'MINUTE', 'MONTH', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NO', 'NORMALIZE', 'NOT', 'NULL', 'NULLIF', 'NULLS', " +
                         "'ONLY', 'OPTION', 'ORDINALITY', 'OUTPUT', 'OVER', 'PARTITION', 'PARTITIONS', 'POSITION', 'PRECEDING', 'PRIVILEGES', 'PROPERTIES', 'PUBLIC', 'RANGE', 'READ', " +
                         "'RENAME', 'REPEATABLE', 'REPLACE', 'RESET', 'RESTRICT', 'REVOKE', 'ROLLBACK', 'ROW', 'ROWS', 'SCHEMA', 'SCHEMAS', 'SECOND', 'SERIALIZABLE', 'SESSION', 'SET', " +
                         "'SETS', 'SHOW', 'SMALLINT', 'SOME', 'START', 'STATS', 'SUBSTRING', 'SYSTEM', 'TABLES', 'TABLESAMPLE', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYINT', 'TO', 'TRANSACTION', " +
                         "'TRUE', 'TRY_CAST', 'TYPE', 'UNBOUNDED', 'UNCOMMITTED', 'USE', 'VALIDATE', 'VERBOSE', 'VIEW', 'WORK', 'WRITE', 'YEAR', 'ZONE', '+', '-', STRING, UNICODE_STRING, " +
                         "BINARY_LITERAL, INTEGER_VALUE, DECIMAL_VALUE, DOUBLE_VALUE, IDENTIFIER, DIGIT_IDENTIFIER, QUOTED_IDENTIFIER, BACKQUOTED_IDENTIFIER, DOUBLE_PRECISION}"},
                {"select * from 'oops",
                 "line 1:15: extraneous input ''' expecting {'(', 'ADD', 'ALL', 'ANALYZE', 'ANY', 'ARRAY', 'ASC', 'AT', 'BERNOULLI', 'CALL', 'CASCADE', 'CATALOGS', 'COALESCE', " +
                         "'COLUMN', 'COLUMNS', 'COMMENT', 'COMMIT', 'COMMITTED', 'CURRENT', 'DATA', 'DATE', 'DAY', 'DESC', 'DISTRIBUTED', 'EXCLUDING', 'EXPLAIN', 'FILTER', 'FIRST', " +
                         "'FOLLOWING', 'FORMAT', 'FUNCTIONS', 'GRANT', 'GRANTS', 'GRAPHVIZ', 'HOUR', 'IF', 'INCLUDING', 'INPUT', 'INTEGER', 'INTERVAL', 'ISOLATION', 'LAST', 'LATERAL', " +
                         "'LEVEL', 'LIMIT', 'LOGICAL', 'MAP', 'MINUTE', 'MONTH', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NO', 'NULLIF', 'NULLS', 'ONLY', 'OPTION', 'ORDINALITY', 'OUTPUT', 'OVER', " +
                         "'PARTITION', 'PARTITIONS', 'POSITION', 'PRECEDING', 'PRIVILEGES', 'PROPERTIES', 'PUBLIC', 'RANGE', 'READ', 'RENAME', 'REPEATABLE', 'REPLACE', 'RESET', 'RESTRICT', " +
                         "'REVOKE', 'ROLLBACK', 'ROW', 'ROWS', 'SCHEMA', 'SCHEMAS', 'SECOND', 'SERIALIZABLE', 'SESSION', 'SET', 'SETS', 'SHOW', 'SMALLINT', 'SOME', 'START', 'STATS', " +
                         "'SUBSTRING', 'SYSTEM', 'TABLES', 'TABLESAMPLE', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYINT', 'TO', 'TRANSACTION', 'TRY_CAST', 'TYPE', 'UNBOUNDED', 'UNCOMMITTED', " +
                         "'UNNEST', 'USE', 'VALIDATE', 'VERBOSE', 'VIEW', 'WORK', 'WRITE', 'YEAR', 'ZONE', IDENTIFIER, DIGIT_IDENTIFIER, QUOTED_IDENTIFIER, BACKQUOTED_IDENTIFIER}"},
                {"select *\nfrom x\nfrom",
                 "line 3:1: extraneous input 'from' expecting {<EOF>, '.', ',', 'ADD', 'ALL', 'ANALYZE', 'ANY', 'ARRAY', 'AS', 'ASC', 'AT', 'BERNOULLI', 'CALL', 'CASCADE', 'CATALOGS', " +
                         "'COALESCE', 'COLUMN', 'COLUMNS', 'COMMENT', 'COMMIT', 'COMMITTED', 'CROSS', 'CURRENT', 'DATA', 'DATE', 'DAY', 'DESC', 'DISTRIBUTED', 'EXCEPT', 'EXCLUDING', " +
                         "'EXPLAIN', 'FILTER', 'FIRST', 'FOLLOWING', 'FORMAT', 'FULL', 'FUNCTIONS', 'GRANT', 'GRANTS', 'GRAPHVIZ', 'GROUP', 'HAVING', 'HOUR', 'IF', 'INCLUDING', 'INNER', " +
                         "'INPUT', 'INTEGER', 'INTERSECT', 'INTERVAL', 'ISOLATION', 'JOIN', 'LAST', 'LATERAL', 'LEFT', 'LEVEL', 'LIMIT', 'LOGICAL', 'MAP', 'MINUTE', 'MONTH', 'NATURAL', " +
                         "'NFC', 'NFD', 'NFKC', 'NFKD', 'NO', 'NULLIF', 'NULLS', 'ONLY', 'OPTION', 'ORDER', 'ORDINALITY', 'OUTPUT', 'OVER', 'PARTITION', 'PARTITIONS', 'POSITION', 'PRECEDING', 'PRIVILEGES', 'PROPERTIES', 'PUBLIC', 'RANGE', 'READ', 'RENAME', 'REPEATABLE', 'REPLACE', 'RESET', 'RESTRICT', 'REVOKE', 'RIGHT', 'ROLLBACK', 'ROW', 'ROWS', 'SCHEMA', 'SCHEMAS', 'SECOND', 'SERIALIZABLE', 'SESSION', 'SET', 'SETS', 'SHOW', 'SMALLINT', 'SOME', 'START', 'STATS', 'SUBSTRING', 'SYSTEM', 'TABLES', 'TABLESAMPLE', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYINT', 'TO', 'TRANSACTION', 'TRY_CAST', 'TYPE', 'UNBOUNDED', 'UNCOMMITTED', 'UNION', 'USE', 'VALIDATE', 'VERBOSE', 'VIEW', 'WHERE', 'WORK', 'WRITE', 'YEAR', 'ZONE', IDENTIFIER, DIGIT_IDENTIFIER, QUOTED_IDENTIFIER, BACKQUOTED_IDENTIFIER}"},
                {"select *\nfrom x\nwhere from",
                 "line 3:7: mismatched input 'from' expecting {'(', '?', 'ADD', 'ALL', 'ANALYZE', 'ANY', 'ARRAY', 'ASC', 'AT', 'BERNOULLI', 'CALL', 'CASCADE', 'CASE', 'CAST', 'CATALOGS', " +
                         "'COALESCE', 'COLUMN', 'COLUMNS', 'COMMENT', 'COMMIT', 'COMMITTED', 'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'DATA', " +
                         "'DATE', 'DAY', 'DESC', 'DISTRIBUTED', 'EXCLUDING', 'EXISTS', 'EXPLAIN', 'EXTRACT', 'FALSE', 'FILTER', 'FIRST', 'FOLLOWING', 'FORMAT', 'FUNCTIONS', 'GRANT', " +
                         "'GRANTS', 'GRAPHVIZ', 'GROUPING', 'HOUR', 'IF', 'INCLUDING', 'INPUT', 'INTEGER', 'INTERVAL', 'ISOLATION', 'LAST', 'LATERAL', 'LEVEL', 'LIMIT', 'LOCALTIME', " +
                         "'LOCALTIMESTAMP', 'LOGICAL', 'MAP', 'MINUTE', 'MONTH', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NO', 'NORMALIZE', 'NOT', 'NULL', 'NULLIF', 'NULLS', 'ONLY', 'OPTION', " +
                         "'ORDINALITY', 'OUTPUT', 'OVER', 'PARTITION', 'PARTITIONS', 'POSITION', 'PRECEDING', 'PRIVILEGES', 'PROPERTIES', 'PUBLIC', 'RANGE', 'READ', 'RENAME', 'REPEATABLE', " +
                         "'REPLACE', 'RESET', 'RESTRICT', 'REVOKE', 'ROLLBACK', 'ROW', 'ROWS', 'SCHEMA', 'SCHEMAS', 'SECOND', 'SERIALIZABLE', 'SESSION', 'SET', 'SETS', 'SHOW', 'SMALLINT', " +
                         "'SOME', 'START', 'STATS', 'SUBSTRING', 'SYSTEM', 'TABLES', 'TABLESAMPLE', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYINT', 'TO', 'TRANSACTION', 'TRUE', 'TRY_CAST', 'TYPE', " +
                         "'UNBOUNDED', 'UNCOMMITTED', 'USE', 'VALIDATE', 'VERBOSE', 'VIEW', 'WORK', 'WRITE', 'YEAR', 'ZONE', '+', '-', STRING, UNICODE_STRING, BINARY_LITERAL, INTEGER_VALUE, " +
                         "DECIMAL_VALUE, DOUBLE_VALUE, IDENTIFIER, DIGIT_IDENTIFIER, QUOTED_IDENTIFIER, BACKQUOTED_IDENTIFIER, DOUBLE_PRECISION}"},
                {"select * from",
                 "line 1:14: no viable alternative at input '<EOF>'"},
                {"select * from  ",
                 "line 1:16: no viable alternative at input '<EOF>'"},
                {"select * from `foo`",
                 "line 1:15: backquoted identifiers are not supported; use double quotes to quote identifiers"},
                {"select * from foo `bar`",
                 "line 1:19: backquoted identifiers are not supported; use double quotes to quote identifiers"},
                {"select 1x from dual",
                 "line 1:8: identifiers must not start with a digit; surround the identifier with double quotes"},
                {"select * from foo@bar",
                 "line 1:15: identifiers must not contain '@'"},
                {"select * from foo:bar",
                 "line 1:15: identifiers must not contain ':'"},
                {"select fuu from dual order by fuu order by fuu",
                 "line 1:35: mismatched input 'order' expecting {<EOF>, '.', ',', '[', 'AND', 'ASC', 'AT', 'BETWEEN', 'DESC', 'IN', 'IS', 'LIKE', 'LIMIT', 'NOT', 'NULLS', 'OR', '=', " +
                         "NEQ, '<', '<=', '>', '>=', '+', '-', '*', '/', '%', '||'}"},
                {"select fuu from dual limit 10 order by fuu",
                 "line 1:31: mismatched input 'order' expecting <EOF>"},
                {"select CAST(12223222232535343423232435343 AS BIGINT)",
                 "line 1:1: Invalid numeric literal: 12223222232535343423232435343"},
                {"select CAST(-12223222232535343423232435343 AS BIGINT)",
                 "line 1:1: Invalid numeric literal: 12223222232535343423232435343"},
                {"select foo(,1)",
                 "line 1:12: no viable alternative at input 'foo(,'"},
                {"select foo(DISTINCT)",
                 "line 1:20: mismatched input ')' expecting {'(', '?', 'ADD', 'ALL', 'ANALYZE', 'ANY', 'ARRAY', 'ASC', 'AT', 'BERNOULLI', 'CALL', 'CASCADE', 'CASE', 'CAST', 'CATALOGS', " +
                         "'COALESCE', 'COLUMN', 'COLUMNS', 'COMMENT', 'COMMIT', 'COMMITTED', 'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'DATA', 'DATE', " +
                         "'DAY', 'DESC', 'DISTRIBUTED', 'EXCLUDING', 'EXISTS', 'EXPLAIN', 'EXTRACT', 'FALSE', 'FILTER', 'FIRST', 'FOLLOWING', 'FORMAT', 'FUNCTIONS', 'GRANT', 'GRANTS', " +
                         "'GRAPHVIZ', 'GROUPING', 'HOUR', 'IF', 'INCLUDING', 'INPUT', 'INTEGER', 'INTERVAL', 'ISOLATION', 'LAST', 'LATERAL', 'LEVEL', 'LIMIT', 'LOCALTIME', 'LOCALTIMESTAMP', " +
                         "'LOGICAL', 'MAP', 'MINUTE', 'MONTH', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NO', 'NORMALIZE', 'NOT', 'NULL', 'NULLIF', 'NULLS', 'ONLY', 'OPTION', 'ORDINALITY', 'OUTPUT', " +
                         "'OVER', 'PARTITION', 'PARTITIONS', 'POSITION', 'PRECEDING', 'PRIVILEGES', 'PROPERTIES', 'PUBLIC', 'RANGE', 'READ', 'RENAME', 'REPEATABLE', 'REPLACE', 'RESET', " +
                         "'RESTRICT', 'REVOKE', 'ROLLBACK', 'ROW', 'ROWS', 'SCHEMA', 'SCHEMAS', 'SECOND', 'SERIALIZABLE', 'SESSION', 'SET', 'SETS', 'SHOW', 'SMALLINT', 'SOME', 'START', " +
                         "'STATS', 'SUBSTRING', 'SYSTEM', 'TABLES', 'TABLESAMPLE', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYINT', 'TO', 'TRANSACTION', 'TRUE', 'TRY_CAST', 'TYPE', 'UNBOUNDED', " +
                         "'UNCOMMITTED', 'USE', 'VALIDATE', 'VERBOSE', 'VIEW', 'WORK', 'WRITE', 'YEAR', 'ZONE', '+', '-', STRING, UNICODE_STRING, BINARY_LITERAL, INTEGER_VALUE, DECIMAL_VALUE, " +
                         "DOUBLE_VALUE, IDENTIFIER, DIGIT_IDENTIFIER, QUOTED_IDENTIFIER, BACKQUOTED_IDENTIFIER, DOUBLE_PRECISION}"},
                {"select foo(DISTINCT ,1)",
                 "line 1:21: extraneous input ',' expecting {'(', '?', 'ADD', 'ALL', 'ANALYZE', 'ANY', 'ARRAY', 'ASC', 'AT', 'BERNOULLI', 'CALL', 'CASCADE', 'CASE', 'CAST', 'CATALOGS', " +
                         "'COALESCE', 'COLUMN', 'COLUMNS', 'COMMENT', 'COMMIT', 'COMMITTED', 'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'DATA', 'DATE', " +
                         "'DAY', 'DESC', 'DISTRIBUTED', 'EXCLUDING', 'EXISTS', 'EXPLAIN', 'EXTRACT', 'FALSE', 'FILTER', 'FIRST', 'FOLLOWING', 'FORMAT', 'FUNCTIONS', 'GRANT', 'GRANTS', " +
                         "'GRAPHVIZ', 'GROUPING', 'HOUR', 'IF', 'INCLUDING', 'INPUT', 'INTEGER', 'INTERVAL', 'ISOLATION', 'LAST', 'LATERAL', 'LEVEL', 'LIMIT', 'LOCALTIME', 'LOCALTIMESTAMP', " +
                         "'LOGICAL', 'MAP', 'MINUTE', 'MONTH', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NO', 'NORMALIZE', 'NOT', 'NULL', 'NULLIF', 'NULLS', 'ONLY', 'OPTION', 'ORDINALITY', 'OUTPUT', " +
                         "'OVER', 'PARTITION', 'PARTITIONS', 'POSITION', 'PRECEDING', 'PRIVILEGES', 'PROPERTIES', 'PUBLIC', 'RANGE', 'READ', 'RENAME', 'REPEATABLE', 'REPLACE', 'RESET', " +
                         "'RESTRICT', 'REVOKE', 'ROLLBACK', 'ROW', 'ROWS', 'SCHEMA', 'SCHEMAS', 'SECOND', 'SERIALIZABLE', 'SESSION', 'SET', 'SETS', 'SHOW', 'SMALLINT', 'SOME', 'START', " +
                         "'STATS', 'SUBSTRING', 'SYSTEM', 'TABLES', 'TABLESAMPLE', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYINT', 'TO', 'TRANSACTION', 'TRUE', 'TRY_CAST', 'TYPE', 'UNBOUNDED', " +
                         "'UNCOMMITTED', 'USE', 'VALIDATE', 'VERBOSE', 'VIEW', 'WORK', 'WRITE', 'YEAR', 'ZONE', '+', '-', STRING, UNICODE_STRING, BINARY_LITERAL, INTEGER_VALUE, DECIMAL_VALUE, " +
                         "DOUBLE_VALUE, IDENTIFIER, DIGIT_IDENTIFIER, QUOTED_IDENTIFIER, BACKQUOTED_IDENTIFIER, DOUBLE_PRECISION}"},
                {"CREATE TABLE foo () AS (VALUES 1)",
                 "line 1:19: no viable alternative at input 'CREATE TABLE foo ()'"},
                {"CREATE TABLE foo (*) AS (VALUES 1)",
                 "line 1:19: no viable alternative at input 'CREATE TABLE foo (*'"},
                {"SELECT grouping(a+2) FROM (VALUES (1)) AS t (a) GROUP BY a+2",
                 "line 1:18: mismatched input '+' expecting {'.', ')', ','}"},
                {"SELECT x() over (ROWS select) FROM t",
                 "line 1:23: no viable alternative at input 'ROWS select'"},
                {"SELECT X() OVER (ROWS UNBOUNDED) FROM T",
                 "line 1:32: no viable alternative at input 'UNBOUNDED)'"},
                {"SELECT a FROM x ORDER BY (SELECT b FROM t WHERE ",
                 "line 1:49: no viable alternative at input '<EOF>'"},
                {"SELECT a FROM a AS x TABLESAMPLE x ",
                 "line 1:34: mismatched input 'x' expecting {'BERNOULLI', 'SYSTEM'}"},
                {"SELECT a AS z FROM t GROUP BY CUBE (a), ",
                 "line 1:41: no viable alternative at input '<EOF>'"},
                {"SELECT a AS z FROM t WHERE x = 1 + ",
                 "line 1:36: no viable alternative at input '<EOF>'"},
                {"SELECT a AS z FROM t WHERE a. ",
                 "line 1:31: no viable alternative at input '<EOF>'"},
                {"CREATE TABLE t (x bigint) COMMENT ",
                 "line 1:35: no viable alternative at input '<EOF>'"},
                {"SELECT * FROM ( ",
                 "line 1:17: no viable alternative at input '( '"},
                {"SELECT CAST(a AS )",
                 "line 1:18: mismatched input ')' expecting {'ADD', 'ALL', 'ANALYZE', 'ANY', 'ARRAY', 'ASC', 'AT', 'BERNOULLI', 'CALL', 'CASCADE', 'CATALOGS', 'COALESCE', 'COLUMN', " +
                         "'COLUMNS', 'COMMENT', 'COMMIT', 'COMMITTED', 'CURRENT', 'DATA', 'DATE', 'DAY', 'DESC', 'DISTRIBUTED', 'EXCLUDING', 'EXPLAIN', 'FILTER', 'FIRST', 'FOLLOWING', " +
                         "'FORMAT', 'FUNCTIONS', 'GRANT', 'GRANTS', 'GRAPHVIZ', 'HOUR', 'IF', 'INCLUDING', 'INPUT', 'INTEGER', 'INTERVAL', 'ISOLATION', 'LAST', 'LATERAL', 'LEVEL', 'LIMIT', " +
                         "'LOGICAL', 'MAP', 'MINUTE', 'MONTH', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NO', 'NULLIF', 'NULLS', 'ONLY', 'OPTION', 'ORDINALITY', 'OUTPUT', 'OVER', 'PARTITION', " +
                         "'PARTITIONS', 'POSITION', 'PRECEDING', 'PRIVILEGES', 'PROPERTIES', 'PUBLIC', 'RANGE', 'READ', 'RENAME', 'REPEATABLE', 'REPLACE', 'RESET', 'RESTRICT', 'REVOKE', " +
                         "'ROLLBACK', 'ROW', 'ROWS', 'SCHEMA', 'SCHEMAS', 'SECOND', 'SERIALIZABLE', 'SESSION', 'SET', 'SETS', 'SHOW', 'SMALLINT', 'SOME', 'START', 'STATS', 'SUBSTRING', " +
                         "'SYSTEM', 'TABLES', 'TABLESAMPLE', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYINT', 'TO', 'TRANSACTION', 'TRY_CAST', 'TYPE', 'UNBOUNDED', 'UNCOMMITTED', 'USE', 'VALIDATE', " +
                         "'VERBOSE', 'VIEW', 'WORK', 'WRITE', 'YEAR', 'ZONE', IDENTIFIER, DIGIT_IDENTIFIER, QUOTED_IDENTIFIER, BACKQUOTED_IDENTIFIER, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, " +
                         "DOUBLE_PRECISION}"},
                {"SELECT CAST(a AS decimal()",
                 "line 1:26: mismatched input ')' expecting {'ADD', 'ALL', 'ANALYZE', 'ANY', 'ARRAY', 'ASC', 'AT', 'BERNOULLI', 'CALL', 'CASCADE', 'CATALOGS', 'COALESCE', 'COLUMN', " +
                         "'COLUMNS', 'COMMENT', 'COMMIT', 'COMMITTED', 'CURRENT', 'DATA', 'DATE', 'DAY', 'DESC', 'DISTRIBUTED', 'EXCLUDING', 'EXPLAIN', 'FILTER', 'FIRST', 'FOLLOWING', " +
                         "'FORMAT', 'FUNCTIONS', 'GRANT', 'GRANTS', 'GRAPHVIZ', 'HOUR', 'IF', 'INCLUDING', 'INPUT', 'INTEGER', 'INTERVAL', 'ISOLATION', 'LAST', 'LATERAL', 'LEVEL', 'LIMIT', " +
                         "'LOGICAL', 'MAP', 'MINUTE', 'MONTH', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NO', 'NULLIF', 'NULLS', 'ONLY', 'OPTION', 'ORDINALITY', 'OUTPUT', 'OVER', 'PARTITION', " +
                         "'PARTITIONS', 'POSITION', 'PRECEDING', 'PRIVILEGES', 'PROPERTIES', 'PUBLIC', 'RANGE', 'READ', 'RENAME', 'REPEATABLE', 'REPLACE', 'RESET', 'RESTRICT', 'REVOKE', " +
                         "'ROLLBACK', 'ROW', 'ROWS', 'SCHEMA', 'SCHEMAS', 'SECOND', 'SERIALIZABLE', 'SESSION', 'SET', 'SETS', 'SHOW', 'SMALLINT', 'SOME', 'START', 'STATS', 'SUBSTRING', " +
                         "'SYSTEM', 'TABLES', 'TABLESAMPLE', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYINT', 'TO', 'TRANSACTION', 'TRY_CAST', 'TYPE', 'UNBOUNDED', 'UNCOMMITTED', 'USE', 'VALIDATE', " +
                         "'VERBOSE', 'VIEW', 'WORK', 'WRITE', 'YEAR', 'ZONE', INTEGER_VALUE, IDENTIFIER, DIGIT_IDENTIFIER, QUOTED_IDENTIFIER, BACKQUOTED_IDENTIFIER, TIME_WITH_TIME_ZONE, " +
                         "TIMESTAMP_WITH_TIME_ZONE, DOUBLE_PRECISION}"},
                {"SELECT foo(*) filter (",
                 "line 1:23: mismatched input '<EOF>' expecting 'WHERE'"},
                {"SELECT * FROM t t x",
                 "line 1:19: extraneous input 'x' expecting {<EOF>, '(', ',', 'CROSS', 'EXCEPT', 'FULL', 'GROUP', 'HAVING', 'INNER', 'INTERSECT', 'JOIN', 'LEFT', 'LIMIT', 'NATURAL', " +
                         "'ORDER', 'RIGHT', 'TABLESAMPLE', 'UNION', 'WHERE'}"},
                {"SELECT * FROM t WHERE EXISTS (",
                 "line 1:31: no viable alternative at input '<EOF>'"},
                };
    }

    @Test(dataProvider = "statements")
    public void testStatement(String sql, String error)
    {
        try {
            SQL_PARSER.createStatement(sql);
            fail("Expected parsing to fail");
        }
        catch (ParsingException e) {
            assertEquals(e.getMessage(), error, "Error message mismatch for query:\n\n" + sql + "\n\n");
        }
    }

    @Test(dataProvider = "expressions")
    public void testExpression(String sql, String error)
    {
        try {
            SQL_PARSER.createExpression(sql);
            fail("Expected parsing to fail");
        }
        catch (ParsingException e) {
            assertEquals(e.getMessage(), error, "Error message mismatch for expression:\n\n" + sql + "\n\n");
        }
    }

    @Test
    public void testParsingExceptionPositionInfo()
    {
        try {
            SQL_PARSER.createStatement("select *\nfrom x\nwhere from");
            fail("expected exception");
        }
        catch (ParsingException e) {
            assertTrue(e.getMessage().startsWith("line 3:7: mismatched input 'from'"));
            assertTrue(e.getErrorMessage().startsWith("mismatched input 'from'"));
            assertEquals(e.getLineNumber(), 3);
            assertEquals(e.getColumnNumber(), 7);
        }
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: expression is too large \\(stack overflow while parsing\\)")
    public void testStackOverflowExpression()
    {
        for (int size = 3000; size <= 100_000; size *= 2) {
            SQL_PARSER.createExpression(Joiner.on(" OR ").join(nCopies(size, "x = y")));
        }
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "line 1:1: statement is too large \\(stack overflow while parsing\\)")
    public void testStackOverflowStatement()
    {
        for (int size = 6000; size <= 100_000; size *= 2) {
            SQL_PARSER.createStatement("SELECT " + Joiner.on(" OR ").join(nCopies(size, "x = y")));
        }
    }
}
