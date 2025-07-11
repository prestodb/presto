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
                {"", "line 1:1: mismatched input '<EOF>'. Expecting: <expression>"},
                {"1 + 1 x", "line 1:7: mismatched input 'x'. Expecting: '%', '*', '+', '-', '.', '/', 'AT', '[', '||', <expression>"}};
    }

    @DataProvider(name = "statements")
    public Object[][] getStatements()
    {
        return new Object[][] {
                {"",
                 "line 1:1: mismatched input '<EOF>'. Expecting: 'ALTER', 'ANALYZE', 'CALL', 'COMMIT', 'CREATE', 'DEALLOCATE', 'DELETE', 'DESC', 'DESCRIBE', 'DROP', 'EXECUTE', 'EXPLAIN', 'GRANT', " +
                         "'INSERT', 'MERGE', 'PREPARE', 'REFRESH', 'RESET', 'REVOKE', 'ROLLBACK', 'SET', 'SHOW', 'START', 'TRUNCATE', 'UPDATE', 'USE', <query>"},
                {"@select",
                 "line 1:1: mismatched input '@'. Expecting: 'ALTER', 'ANALYZE', 'CALL', 'COMMIT', 'CREATE', 'DEALLOCATE', 'DELETE', 'DESC', 'DESCRIBE', 'DROP', 'EXECUTE', 'EXPLAIN', 'GRANT', " +
                         "'INSERT', 'MERGE', 'PREPARE', 'REFRESH', 'RESET', 'REVOKE', 'ROLLBACK', 'SET', 'SHOW', 'START', 'TRUNCATE', 'UPDATE', 'USE', <query>"},
                {"select * from foo where @what",
                 "line 1:25: mismatched input '@'. Expecting: <expression>"},
                {"select * from 'oops",
                 "line 1:15: mismatched input '''. Expecting: '(', 'LATERAL', 'UNNEST', <identifier>"},
                {"select *\nfrom x\nfrom",
                 "line 3:1: mismatched input 'from'. Expecting: ',', '.', 'AS', 'CROSS', 'EXCEPT', 'FETCH', 'FOR', 'FULL', 'GROUP', 'HAVING', 'INNER', 'INTERSECT', 'JOIN', 'LEFT', 'LIMIT', 'NATURAL', 'OFFSET', 'ORDER', 'RIGHT', 'TABLESAMPLE', 'UNION', 'WHERE', <EOF>, <identifier>"},
                {"select *\nfrom x\nwhere from",
                 "line 3:7: mismatched input 'from'. Expecting: <expression>"},
                {"select * from",
                 "line 1:14: mismatched input '<EOF>'. Expecting: '(', 'LATERAL', 'UNNEST', <identifier>"},
                {"select * from  ",
                 "line 1:16: mismatched input '<EOF>'. Expecting: '(', 'LATERAL', 'UNNEST', <identifier>"},
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
                 "line 1:35: mismatched input 'order'. Expecting: '%', '*', '+', '-', '.', '/', 'AT', '[', '||', <expression>"},
                {"select fuu from dual limit 10 order by fuu",
                 "line 1:31: mismatched input 'order'. Expecting: <EOF>"},
                {"select CAST(12223222232535343423232435343 AS BIGINT)",
                 "line 1:1: Invalid numeric literal: 12223222232535343423232435343"},
                {"select CAST(-12223222232535343423232435343 AS BIGINT)",
                 "line 1:1: Invalid numeric literal: 12223222232535343423232435343"},
                {"select foo(,1)",
                 "line 1:12: mismatched input ','. Expecting: <expression>"},
                {"select foo(DISTINCT)",
                 "line 1:20: mismatched input ')'. Expecting: <expression>"},
                {"select foo(DISTINCT ,1)",
                 "line 1:21: mismatched input ','. Expecting: <expression>"},
                {"CREATE TABLE foo () AS (VALUES 1)",
                 "line 1:19: mismatched input ')'. Expecting: 'FUNCTION', 'MATERIALIZED', 'OR', 'ROLE', 'SCHEMA', 'TABLE', 'TEMPORARY', 'TYPE', 'VIEW'"},
                {"CREATE TABLE foo (*) AS (VALUES 1)",
                 "line 1:19: mismatched input '*'. Expecting: 'FUNCTION', 'MATERIALIZED', 'OR', 'ROLE', 'SCHEMA', 'TABLE', 'TEMPORARY', 'TYPE', 'VIEW'"},
                {"SELECT grouping(a+2) FROM (VALUES (1)) AS t (a) GROUP BY a+2",
                 "line 1:18: mismatched input '+'. Expecting: ')', ','"},
                {"SELECT x() over (ROWS select) FROM t",
                 "line 1:17: mismatched input '('. Expecting: ',', 'EXCEPT', 'FETCH', 'FROM', 'GROUP', 'HAVING', 'INTERSECT', 'LIMIT', 'OFFSET', 'ORDER', 'UNION', 'WHERE', <EOF>"},
                {"SELECT X() OVER (ROWS UNBOUNDED) FROM T",
                 "line 1:32: mismatched input ')'. Expecting: 'FOLLOWING', 'PRECEDING'"},
                {"SELECT a FROM x ORDER BY (SELECT b FROM t WHERE ",
                 "line 1:49: mismatched input '<EOF>'. Expecting: <expression>"},
                {"SELECT a FROM a AS x TABLESAMPLE x ",
                 "line 1:34: mismatched input 'x'. Expecting: 'BERNOULLI', 'SYSTEM'"},
                {"SELECT a AS z FROM t GROUP BY CUBE (a), ",
                 "line 1:41: mismatched input '<EOF>'. Expecting: '(', 'CUBE', 'GROUPING', 'ROLLUP', <expression>"},
                {"SELECT a AS z FROM t WHERE x = 1 + ",
                 "line 1:36: mismatched input '<EOF>'. Expecting: <expression>"},
                {"SELECT a AS z FROM t WHERE a. ",
                 "line 1:31: mismatched input '<EOF>'. Expecting: <identifier>"},
                {"CREATE TABLE t (x bigint) COMMENT ",
                 "line 1:35: mismatched input '<EOF>'. Expecting: <string>"},
                {"SELECT * FROM ( ",
                 "line 1:17: mismatched input '<EOF>'. Expecting: '(', 'LATERAL', 'UNNEST', <identifier>, <query>"},
                {"SELECT CAST(a AS )",
                 "line 1:18: mismatched input ')'. Expecting: <type>"},
                {"SELECT CAST(a AS decimal()",
                 "line 1:26: mismatched input ')'. Expecting: <integer>, <type>"},
                {"SELECT foo(*) filter (",
                 "line 1:22: mismatched input '('. Expecting: ',', 'EXCEPT', 'FETCH', 'FROM', 'GROUP', 'HAVING', 'INTERSECT', 'LIMIT', 'OFFSET', 'ORDER', 'UNION', 'WHERE', <EOF>"},
                {"SELECT * FROM t t x",
                 "line 1:19: mismatched input 'x'. Expecting: '(', ',', 'CROSS', 'EXCEPT', 'FETCH', 'FULL', 'GROUP', 'HAVING', 'INNER', 'INTERSECT', 'JOIN', 'LEFT', 'LIMIT', 'NATURAL', 'OFFSET', 'ORDER', " +
                         "'RIGHT', 'TABLESAMPLE', 'UNION', 'WHERE', <EOF>"},
                {"SELECT * FROM t WHERE EXISTS (",
                 "line 1:31: mismatched input '<EOF>'. Expecting: <query>"},
                {"SHOW SESSION LIKE '%$_%' ESCAPE",
                        "line 1:32: mismatched input '<EOF>'. Expecting: <string>"},
                {"SHOW CATALOGS LIKE '%$_%' ESCAPE",
                        "line 1:33: mismatched input '<EOF>'. Expecting: <string>"}};
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
