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
package com.facebook.presto.tests.sql;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;

public class TestTable
        implements AutoCloseable
{
    private static final SecureRandom random = new SecureRandom();
    // The suffix needs to be long enough to "prevent" collisions in practice. The length of 5 was proven not to be long enough
    private static final int RANDOM_SUFFIX_LENGTH = 10;

    private final SqlExecutor sqlExecutor;
    private final String name;

    private static final AtomicInteger instanceCounter = new AtomicInteger();

    public TestTable(SqlExecutor sqlExecutor, String namePrefix, String createDdlTemplate)
    {
        this.sqlExecutor = sqlExecutor;
        this.name = namePrefix + "_" + instanceCounter.incrementAndGet();
        sqlExecutor.execute(createDdlTemplate.replace("{TABLE_NAME}", this.name));
    }

    public String getName()
    {
        return name;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP TABLE " + name);
    }

    public static String randomTableSuffix()
    {
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }
}
