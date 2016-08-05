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

import java.util.concurrent.atomic.AtomicInteger;

public class TestTable
        implements AutoCloseable
{
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
}
