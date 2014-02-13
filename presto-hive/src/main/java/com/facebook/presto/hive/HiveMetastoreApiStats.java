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
package com.facebook.presto.hive;

import com.facebook.presto.hive.shaded.org.apache.thrift.TException;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Callable;

@ThreadSafe
public class HiveMetastoreApiStats
{
    private final TimeStat time = new TimeStat();
    private final CounterStat totalFailures = new CounterStat();
    private final CounterStat metastoreExceptions = new CounterStat();
    private final CounterStat thriftExceptions = new CounterStat();

    public <V> Callable<V> wrap(final Callable<V> callable)
    {
        return new Callable<V>()
        {
            @Override
            public V call()
                    throws Exception
            {
                try (TimeStat.BlockTimer timer = time.time()) {
                    return callable.call();
                }
                catch (Exception e) {
                    if (e instanceof MetaException) {
                        metastoreExceptions.update(1);
                    }
                    else if (e instanceof TException) {
                        thriftExceptions.update(1);
                    }
                    totalFailures.update(1);

                    throw e;
                }
            }
        };
    }

    @Managed
    @Nested
    public TimeStat getTime()
    {
        return time;
    }

    @Managed
    @Nested
    public CounterStat getTotalFailures()
    {
        return totalFailures;
    }

    @Managed
    @Nested
    public CounterStat getThriftExceptions()
    {
        return thriftExceptions;
    }

    @Managed
    @Nested
    public CounterStat getMetastoreExceptions()
    {
        return metastoreExceptions;
    }
}
