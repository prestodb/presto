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
package com.facebook.presto.lance;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestLanceWritableTableHandle
{
    @Test
    public void testProperties()
    {
        List<LanceColumnHandle> columns = ImmutableList.of(
                new LanceColumnHandle("id", INTEGER, false),
                new LanceColumnHandle("name", VARCHAR, true));
        LanceWritableTableHandle handle = new LanceWritableTableHandle(
                "default", "test_table", "{}", columns);

        assertEquals(handle.getSchemaName(), "default");
        assertEquals(handle.getTableName(), "test_table");
        assertEquals(handle.getSchemaJson(), "{}");
        assertEquals(handle.getInputColumns().size(), 2);
        assertEquals(handle.getInputColumns().get(0).getColumnName(), "id");
    }
}
