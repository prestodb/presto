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
package com.facebook.plugin.arrow;

import com.facebook.presto.testing.MaterializedResult;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestArrowFlightQueriesWithTVF
        extends TestArrowFlightQueries
{
    @Test
    public void testQueryFunctionWithRestrictedColumns()
    {
        MaterializedResult actualRow = computeActual("SELECT id from TABLE(system.query_function('SELECT name, id FROM tpch.member WHERE id = 1', 'name VARCHAR, id INTEGER'))");
        MaterializedResult expectedRow = resultBuilder(getSession(), INTEGER)
                .row(1)
                .build();
        assertEquals(actualRow, expectedRow);
    }

    @Test
    public void testQueryFunctionWithoutRestrictedColumns()
    {
        MaterializedResult actualRow = computeActual("SELECT id, name from TABLE(system.query_function('SELECT name, id FROM tpch.member WHERE id = 1', 'name VARCHAR, id INTEGER'))");
        MaterializedResult expectedRow = resultBuilder(getSession(), INTEGER, VARCHAR)
                .row(1, "TOM")
                .build();
        assertEquals(actualRow, expectedRow);
    }
}
