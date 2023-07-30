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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class TestReplaceFirstFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction(
                "REPLACE_FIRST('aaa', 'a', 'b')",
                VARCHAR,
                "baa");
        assertFunction(
                "REPLACE_FIRST('replace_all', 'all', 'first')",
                VARCHAR,
                "replace_first");
        assertFunction(
                "REPLACE_FIRST('The quick brown dog jumps over a lazy dog', 'dog', 'fox')",
                VARCHAR,
                "The quick brown fox jumps over a lazy dog");
        assertFunction(
                "REPLACE_FIRST('John  Doe', ' ', '')",
                VARCHAR,
                "John Doe");
        assertFunction(
                "REPLACE_FIRST('We will fight for our rights, for our rights.', ', for our rights', '')",
                VARCHAR,
                "We will fight for our rights.");
        assertFunction(
                "REPLACE_FIRST('Testcases test cases', 'cases', '')",
                VARCHAR,
                "Test test cases");
        assertFunction(
                "REPLACE_FIRST('test cases', '', 'Add ')",
                VARCHAR,
                "Add test cases");
    }

    @Test
    public void testEmpty()
    {
        assertFunction("REPLACE_FIRST('', 'a', 'b')", VARCHAR, "");
        assertFunction("REPLACE_FIRST('', '', 'test')", VARCHAR, "test");
        assertFunction("REPLACE_FIRST('', 'a', '')", VARCHAR, "");
    }

    @Test
    public void testNull()
    {
        assertFunction("REPLACE_FIRST(NULL, 'foo', 'bar')", VARCHAR, null);
        assertFunction("REPLACE_FIRST('foo', NULL, 'bar')", VARCHAR, null);
        assertFunction("REPLACE_FIRST('foo', 'bar', NULL)", VARCHAR, null);
        assertFunction("REPLACE_FIRST(NULL, NULL, 'test')", VARCHAR, null);
        assertFunction("REPLACE_FIRST('foo', 'NULL', NULL)", VARCHAR, null);
    }

    @Test
    public void testError()
    {
        assertInvalidFunction(
                "REPLACE_FIRST(1000, '1000', '100')",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "REPLACE_FIRST('1000.0', 1000.0, '100')",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "REPLACE_FIRST('1000', '1000', 100)",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "REPLACE_FIRST('replace first')",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "REPLACE_FIRST('replace first', 'first')",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
    }
}
