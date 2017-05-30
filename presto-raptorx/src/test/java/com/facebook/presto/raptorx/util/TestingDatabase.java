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
package com.facebook.presto.raptorx.util;

import org.jdbi.v3.core.ConnectionFactory;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.io.Closeable;
import java.sql.DriverManager;

import static java.lang.String.format;

public final class TestingDatabase
        extends SimpleDatabase
        implements Closeable
{
    private final Handle dummyHandle;

    public TestingDatabase()
    {
        super(Type.H2, createFactory());
        dummyHandle = Jdbi.open(getMasterConnection());
    }

    @Override
    public void close()
    {
        dummyHandle.close();
    }

    private static ConnectionFactory createFactory()
    {
        String url = format("jdbc:h2:mem:test%s;MODE=MySQL", System.nanoTime());
        return () -> DriverManager.getConnection(url);
    }
}
