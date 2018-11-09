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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

@ExtendWith(TestPrecomputedHashes.PrecomputedHashesQueryAssertionsExtension.class)
public class TestPrecomputedHashes
{
    @Test
    public void testDistinctLimit(QueryAssertions assertions)
    {
        // issue #11593
        assertions.assertQuery(
                "SELECT a " +
                        "FROM (" +
                        "    SELECT a, b" +
                        "    FROM (VALUES (1, 2)) t(a, b)" +
                        "    WHERE a = 1" +
                        "    GROUP BY a, b" +
                        "    LIMIT 1" +
                        ")" +
                        "GROUP BY a",
                "VALUES (1)");
    }

    public static class PrecomputedHashesQueryAssertionsExtension
            extends QueryAssertionsExtension
    {
        public PrecomputedHashesQueryAssertionsExtension()
        {
            super(() -> new QueryAssertions(testSessionBuilder()
                    .setSystemProperty(OPTIMIZE_HASH_GENERATION, "true")
                    .build()));
        }
    }
}
