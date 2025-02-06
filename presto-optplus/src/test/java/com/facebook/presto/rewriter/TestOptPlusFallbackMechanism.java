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
package com.facebook.presto.rewriter;

import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.stream.Stream;

@Test(singleThreaded = true)
public class TestOptPlusFallbackMechanism
{
    @Test(singleThreaded = true)
    static class TestWithFallbackOn
            extends AbstractOptPlusTestFramework
    {
        @Override
        protected boolean enableFallback()
        {
            return true;
        }

        @Override
        protected Stream<String> getQueryPaths()
        {
            return ImmutableList.of("/test_queries/query_fail_only_in_optplus.sql").stream();
        }
    }

    @Test(singleThreaded = true)
    static class TestWithFallbackOff
            extends AbstractOptPlusTestFramework
    {
        @Override
        @Test(timeOut = 240_000, dataProvider = "getQueriesDataProvider")
        public void testQueries(String queryResourcePath)
        {
            assertQueryFails(read(queryResourcePath), ".*DB2 SQL Error: SQLCODE=-440, SQLSTATE=42884, SQLERRMC=STARTS_WITH;FUNCTION.*");
        }

        @Override
        protected boolean enableFallback()
        {
            return false;
        }

        @Override
        protected Stream<String> getQueryPaths()
        {
            return ImmutableList.of("/test_queries/query_fail_only_in_optplus.sql").stream();
        }
    }
}
