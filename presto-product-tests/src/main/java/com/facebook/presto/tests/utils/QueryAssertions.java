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
package com.facebook.presto.tests.utils;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import io.airlift.units.Duration;
import io.prestodb.tempto.query.QueryResult;

import java.util.function.Supplier;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.fail;

public class QueryAssertions
{
    public static void assertContainsEventually(Supplier<QueryResult> all, QueryResult expectedSubset, Duration timeout)
    {
        long start = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                assertContains(all.get(), expectedSubset);
                return;
            }
            catch (AssertionError e) {
                if (nanosSince(start).compareTo(timeout) > 0) {
                    throw e;
                }
            }
            sleepUninterruptibly(50, MILLISECONDS);
        }
    }

    public static void assertContains(QueryResult all, QueryResult expectedSubset)
    {
        for (Object row : expectedSubset.rows()) {
            if (!all.rows().contains(row)) {
                fail(format("expected row missing: %s\nAll %s rows:\n    %s\nExpected subset %s rows:\n    %s\n",
                        row,
                        all.getRowsCount(),
                        Joiner.on("\n    ").join(Iterables.limit(all.rows(), 100)),
                        expectedSubset.getRowsCount(),
                        Joiner.on("\n    ").join(Iterables.limit(expectedSubset.rows(), 100))));
            }
        }
    }

    private QueryAssertions() {}
}
