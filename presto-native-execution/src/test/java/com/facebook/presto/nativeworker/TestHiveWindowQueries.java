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
package com.facebook.presto.nativeworker;

import org.testng.annotations.Test;

public class TestHiveWindowQueries
        extends AbstractTestHiveQueries
{
    public TestHiveWindowQueries()
    {
        super(true);
    }

    @Test(enabled = false)
    public void testWindow()
    {
        assertQueryFails("SELECT clerk, orderdate, orderkey, totalprice, rank() OVER (PARTITION BY clerk ORDER BY totalprice) AS rnk FROM orders ORDER BY clerk, rnk", ".*Unknown plan node type com.facebook.presto.sql.planner.plan.WindowNode.*");
    }
}
