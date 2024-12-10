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
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.Iterables;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMapVarcharJsonOperator
{
    private QueryRunner queryRunner;
    @BeforeClass
    public void setUp()
    {
        Session session = testSessionBuilder().build();
        this.queryRunner = new LocalQueryRunner(session);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testFunction()
    {
        Map<String, String> map = new HashMap<>();
        map.put("m", "[\"rn\",\"w\",\"a\"]");
        assertThatQueryReturnsValue("SELECT TRY(CAST(json_parse(c0) AS map(varchar, json))) from (values ('{\"m\": [\"rn\", \"w\", \"a\"]}')) t(c0)", map);
        map.put("m", "{\"pl\":\"4\",\"rn\":\"w\"}");
        assertThatQueryReturnsValue("SELECT TRY(CAST(json_parse(c0) AS map(varchar, json))) from (values ('{\"m\": {\"rn\": \"w\", \"pl\": \"4\"}}')) t(c0)", map);
    }

    private void assertThatQueryReturnsValue(@Language("SQL") String sql, Object expected)
    {
        MaterializedResult rows = queryRunner.execute(sql);
        MaterializedRow materializedRow = Iterables.getOnlyElement(rows);
        int fieldCount = materializedRow.getFieldCount();
        assertTrue(fieldCount == 1, format("Expected only one column, but got '%d'", fieldCount));
        Object value = materializedRow.getField(0);
        assertEquals(value, expected);
        assertTrue(Iterables.getOnlyElement(rows).getFieldCount() == 1);
    }
}
