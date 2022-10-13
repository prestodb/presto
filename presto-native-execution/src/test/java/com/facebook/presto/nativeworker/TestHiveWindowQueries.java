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

    @Test
    public void testWindow()
    {
        assertQuery("SELECT clerk, orderdate, orderkey, totalprice, row_number() OVER (PARTITION BY clerk ORDER BY totalprice) AS rnk FROM orders ORDER BY clerk, rnk");
        assertQuery("SELECT clerk, orderdate, orderkey, totalprice, rank() OVER (PARTITION BY clerk ORDER BY totalprice) AS rnk FROM orders ORDER BY clerk, rnk");
        assertQuery("SELECT clerk, orderdate, orderkey, totalprice, dense_rank() OVER (PARTITION BY clerk ORDER BY totalprice) AS rnk FROM orders ORDER BY clerk, rnk");
        assertQuery("SELECT clerk, orderdate, orderkey, totalprice, percent_rank() OVER (PARTITION BY clerk ORDER BY totalprice) AS rnk FROM orders ORDER BY clerk, rnk");
        assertQuery("SELECT clerk, orderdate, orderkey, totalprice, cume_dist() OVER (PARTITION BY clerk ORDER BY totalprice) AS rnk FROM orders ORDER BY clerk, rnk");
    }
}
