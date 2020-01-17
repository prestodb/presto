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
package com.facebook.presto.tests;

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestGroupByUsesEqualTo
{
    private QueryRunner runner;

    @BeforeClass
    public void setUp()
    {
        runner = createQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (runner != null) {
            runner.close();
            runner = null;
        }
    }

    @Test
    public void testLegacyGroupBy()
    {
        MaterializedResult result = runner.execute("select * from (values nan(), nan(), nan()) group by 1");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(rows.size(), 3);
        rows.stream()
                .forEach(row -> assertTrue(Double.isNaN((Double) row.getField(0))));
    }

    private static QueryRunner createQueryRunner()
    {
        return new LocalQueryRunner(testSessionBuilder().build(), new FeaturesConfig().setGroupByUsesEqualTo(true));
    }
}
