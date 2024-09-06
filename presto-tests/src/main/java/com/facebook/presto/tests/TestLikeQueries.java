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

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestLikeQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("test")
                .setSchema("default")
                .build();
        return new DistributedQueryRunner(session, 1);
    }

    @Test
    public void testLikeQueriesWithEscape()
    {
        assertQuery("SELECT IF('xT' LIKE '#_T' ESCAPE '#', c0, c1) FROM (values (true, false)) t(c0, c1)", "SELECT false");
        assertQuery("SELECT IF(c2 LIKE 'T' ESCAPE '#', c0, c1) FROM (values (true, false, 'T')) t(c0, c1, c2)", "SELECT true");
    }

    @Test
    public void testLikeQueriesWithInvalidEscape()
    {
        assertQueryFails("SELECT IF('T' LIKE '###T' ESCAPE '##', c0, c1) FROM (values (true, false)) t(c0, c1)", ".*Escape string must be a single character$");
        assertQueryFails("SELECT IF(c2 LIKE '' ESCAPE '', c0, c1) FROM (values (true, false, 'T')) t(c0, c1, c2)", ".*Escape string must be a single character$");
    }
}
