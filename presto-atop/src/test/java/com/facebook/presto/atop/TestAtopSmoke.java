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

package com.facebook.presto.atop;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.Iterables;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.atop.LocalAtopQueryRunner.createQueryRunner;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAtopSmoke
{
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @AfterClass
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testDisks()
    {
        assertThatQueryReturnsValue("SELECT device_name FROM disks LIMIT 1", "sda");
    }

    @Test
    public void testPredicatePushdown()
    {
        assertThatQueryReturnsValue("SELECT device_name FROM disks WHERE start_time < current_timestamp LIMIT 1", "sda");
    }

    @Test
    public void testReboots()
    {
        assertThatQueryReturnsValue("SELECT count(*) FROM reboots WHERE CAST(power_on_time AS date) = current_date", 2L);
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
