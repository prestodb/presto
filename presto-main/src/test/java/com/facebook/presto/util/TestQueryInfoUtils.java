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
package com.facebook.presto.util;

import org.testng.annotations.Test;

import static com.facebook.presto.util.QueryInfoUtils.computeQueryHash;
import static com.google.common.base.Strings.repeat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQueryInfoUtils
{
    @Test
    public void testComputeQueryHash()
    {
        String query = "SELECT * FROM CUSTOMER LIMIT 5";
        assertEquals(computeQueryHash(query), "7f3325a942b43504");
    }

    @Test
    public void testComputeQueryHashLongQuery()
    {
        String longQuery = "SELECT x" + repeat(",x", 500_000) + " FROM (VALUES 1,2,3,4,5) t(x)";
        assertEquals(computeQueryHash(longQuery), "26691c4d35d09c7b");
    }

    @Test
    public void testComputeQueryHashEquivalentStrings()
    {
        String query1 = "SELECT * FROM REPORTS WHERE YEAR >= 2000 AND ID = 293";
        String query2 = "SELECT * FROM REPORTS WHERE YEAR >= 2000 AND ID = 293";
        assertEquals(computeQueryHash(query1), computeQueryHash(query2));
    }

    @Test
    public void testComputeQueryHashEmptyString()
    {
        String query = "";
        assertTrue(computeQueryHash(query).isEmpty());
    }
}
