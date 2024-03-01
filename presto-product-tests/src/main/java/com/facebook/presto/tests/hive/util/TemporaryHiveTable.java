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
package com.facebook.presto.tests.hive.util;

import java.security.SecureRandom;

import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class TemporaryHiveTable
        implements AutoCloseable
{
    private static final SecureRandom random = new SecureRandom();
    private static final int RANDOM_SUFFIX_LENGTH = 12;

    public static TemporaryHiveTable temporaryHiveTable(String tableName)
    {
        return new TemporaryHiveTable(tableName);
    }

    public static String randomTableSuffix()
    {
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }

    private final String name;

    private TemporaryHiveTable(String name)
    {
        this.name = requireNonNull(name, "name is null");
    }

    public String getName()
    {
        return name;
    }

    public void closeQuietly(Exception e)
    {
        try (TemporaryHiveTable justCloseIt = this) {
            // suppress empty try-catch warning
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
    }

    @Override
    public void close()
    {
        onHive().executeQuery("DROP TABLE " + name);
    }
}
