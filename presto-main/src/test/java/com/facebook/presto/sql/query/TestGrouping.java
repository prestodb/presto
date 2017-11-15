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
package com.facebook.presto.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestGrouping
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
            throws Exception
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testImplicitCoercions()
            throws Exception
    {
        // GROUPING + implicit coercions (issue #8738)
        assertions.assertQuery(
                "SELECT GROUPING(k), SUM(v) + 1.0 FROM (VALUES (1, 1)) AS t(k,v) GROUP BY k",
                "VALUES (0, 2.0)");
    }
}
