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

import org.testng.annotations.Test;

public abstract class AbstractTestTopN
        extends AbstractTestQueryFramework
{
    @Test
    public void testUngroupedTopN()
    {
        assertQuery("SELECT custkey, totalprice from orders ORDER BY totalprice limit 3");
    }

    @Test
    public void testGroupedTopN()
    {
        assertQuery(
                "SELECT * FROM (SELECT " +
                        "custkey, " +
                        "totalprice, " +
                        "ROW_NUMBER() OVER (PARTITION BY custkey order by totalprice) rn " +
                        "from orders) " +
                        "where rn < 3");
    }

    @Test
    public void testGroupedTopNRowNumber()
    {
        assertQuery(
                "SELECT * FROM (SELECT " +
                        "custkey, " +
                        "totalprice, " +
                        "ROW_NUMBER() OVER (PARTITION BY custkey order by totalprice) rn " +
                    "from orders) " +
                    "where rn < 3");
    }

    @Test
    public void testUnGroupedTopN()
    {
        assertQuery(
                "SELECT * FROM (SELECT " +
                        "custkey, " +
                        "SUM(totalprice) t " +
//                        "ROW_NUMBER() OVER (PARTITION BY custkey order by totalprice) rn " +
                        "from orders GROUP BY custkey ) order by t desc " +
                        "limit 3");
    }

    @Test
    public void testGroupedTopWithAggregationAndMultiChannelGrouping()
    {
        assertQuery(
                "SELECT * FROM " +
                        "( SELECT " +
                        " regionkey, RANK() OVER (PARTITION BY regionkey ORDER BY nation_count) r  FROM" +
                        "   ( SELECT R.regionkey, count(distinct nationkey) nation_count FROM " +
                        "       region R " +
                        "       JOIN nation N ON  R.regionkey=N.regionkey " +
                        "       GROUP BY R.regionkey" +
                        "   )" +
                        ") " +
                    " WHERE " +
                        "r <= 2");
    }
}
