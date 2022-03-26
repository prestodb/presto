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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static java.lang.String.format;
import static org.testng.Assert.fail;

public class TestIcebergSmokeHive
        extends TestAbstractIcebergSmoke
{
    public TestIcebergSmokeHive()
    {
        super(HIVE);
    }

    @Test
    public void testConcurrentInsert()
    {
        final Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_concurrent_insert (col0 INTEGER, col1 VARCHAR) WITH (format = 'ORC')");

        int concurrency = 5;
        final String[] strings = {"one", "two", "three", "four", "five"};
        final CountDownLatch countDownLatch = new CountDownLatch(concurrency);
        AtomicInteger value = new AtomicInteger(0);
        Set<Throwable> errors = new CopyOnWriteArraySet<>();
        List<Thread> threads = Stream.generate(() -> new Thread(() -> {
            int i = value.getAndIncrement();
            try {
                getQueryRunner().execute(session, format("INSERT INTO test_concurrent_insert VALUES(%s, '%s')", i + 1, strings[i]));
            }
            catch (Throwable throwable) {
                errors.add(throwable);
            }
            finally {
                countDownLatch.countDown();
            }
        })).limit(concurrency).collect(Collectors.toList());

        threads.forEach(Thread::start);

        try {
            final int seconds = 10;
            if (!countDownLatch.await(seconds, TimeUnit.SECONDS)) {
                fail(format("Failed to insert in %s seconds", seconds));
            }
            if (!errors.isEmpty()) {
                fail(format("Failed to insert concurrently: %s", errors.stream().map(Throwable::getMessage).collect(Collectors.joining(" & "))));
            }
            assertQuery(session, "SELECT count(*) FROM test_concurrent_insert", "SELECT " + concurrency);
            assertQuery(session, "SELECT * FROM test_concurrent_insert", "VALUES(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')");
        }
        catch (InterruptedException e) {
            fail("Interrupted when await insertion", e);
        }
        finally {
            dropTable(session, "test_concurrent_insert");
        }
    }
}
