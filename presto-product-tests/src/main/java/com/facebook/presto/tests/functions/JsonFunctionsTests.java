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
package com.facebook.presto.tests.functions;

import com.teradata.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.JSON_FUNCTIONS;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.query;

public class JsonFunctionsTests
        extends ProductTest
{
    @Test(groups = JSON_FUNCTIONS)
    public void testJsonArrayContainsExist()
    {
        assertThat(query("SELECT json_array_contains('[1, 2, 3]', 2)")).containsExactly(row(true));
        assertThat(query("SELECT json_array_contains(CAST('[1, 2, 3]' as JSON), 2)")).containsExactly(row(true));
    }

    @Test(groups = JSON_FUNCTIONS)
    public void testJsonArrayLengthExist()
    {
        assertThat(query("SELECT json_array_length('[1, 2, 3]')")).containsExactly(row(3L));
        assertThat(query("SELECT json_array_length(CAST('[1, 2, 3]' as JSON))")).containsExactly(row(3L));
    }

    @Test(groups = JSON_FUNCTIONS)
    public void testJsonExtractExist()
    {
        assertThat(query("SELECT CAST(json_extract('{\"book\" : {\"pages\":1}}', '$.book') as VARCHAR)")).containsExactly(row("{\"pages\":1}"));
    }

    @Test(groups = JSON_FUNCTIONS)
    public void testJsonExtractScalarExist()
    {
        assertThat(query("SELECT json_extract_scalar('{\"book\" : {\"pages\":1}}', '$.book.pages')")).containsExactly(row("1"));
    }

    @Test(groups = JSON_FUNCTIONS)
    public void testJsonArrayGetExist()
    {
        assertThat(query("SELECT CAST (json_array_get('[\"string1\", \"string2\", \"string3\"]', 0) AS VARCHAR)")).containsExactly(row("string1"));
    }

    @Test(groups = JSON_FUNCTIONS)
    public void testJsonSizeExist()
    {
        assertThat(query("SELECT json_size('{ \"x\": {\"a\": 1, \"b\": 2} }', '$.x')")).containsExactly(row(2));
    }
}
