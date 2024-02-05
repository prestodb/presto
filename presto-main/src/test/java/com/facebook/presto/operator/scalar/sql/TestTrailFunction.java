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

public class TestTrailFunction
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction(
                "TRAIL('foobar', 3)",
                VARCHAR,
                "bar");
        assertFunction(
                "TRAIL('foobar', 7)",
                VARCHAR,
                "foobar");
        assertFunction(
                "TRAIL('foobar', 0)",
                VARCHAR,
                "");
        assertFunction(
                "TRAIL('foobar', -1)",
                VARCHAR,
                "");
    }

    @Test
    public void testEmpty()
    {
        assertFunction(
                "TRAIL('', 3)",
                VARCHAR,
                "");
    }

    @Test
    public void testNull()
    {
        assertFunction(
                "TRAIL(CAST(NULL AS VARCHAR), 3)",
                VARCHAR,
                null);
        assertFunction(
                "TRAIL('foobar', NULL)",
                VARCHAR,
                null);
        assertFunction(
                "TRAIL(NULL, 3)", VARCHAR, null);
    }

    @Test
    public void testError()
    {
        assertInvalidFunction(
                "TRAIL('foobar', 'three')",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
        assertInvalidFunction(
                "TRAIL(3, 3)",
                SemanticErrorCode.FUNCTION_NOT_FOUND);
    }
}
