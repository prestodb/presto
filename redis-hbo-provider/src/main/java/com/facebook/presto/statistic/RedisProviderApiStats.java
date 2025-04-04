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
package com.facebook.presto.statistic;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import com.google.errorprone.annotations.ThreadSafe;
import io.lettuce.core.RedisException;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class RedisProviderApiStats
{
    private static final Logger log = Logger.get(RedisProviderApiStats.class);

    enum Operation
    {
        FetchStats,
        PutStats
    }

    private final TimeStat fetchTime = new TimeStat(MILLISECONDS);
    private final TimeStat putTime = new TimeStat(MILLISECONDS);
    private final CounterStat putStatRequest = new CounterStat();
    private final CounterStat putStatGenericFailure = new CounterStat();
    private final CounterStat putStatTimeoutFailure = new CounterStat();
    private final CounterStat putStatRedisFailure = new CounterStat();
    private final CounterStat fetchStatRequest = new CounterStat();
    private final CounterStat fetchStatGenericFailure = new CounterStat();
    private final CounterStat fetchStatTimeoutFailure = new CounterStat();
    private final CounterStat fetchStatRedisFailure = new CounterStat();

    public TimeStat getTime(Operation operation)
    {
        return (operation == Operation.FetchStats) ? fetchTime : putTime;
    }

    public CounterStat getStatRequest(Operation operation)
    {
        return (operation == Operation.FetchStats) ? fetchStatRequest : putStatRequest;
    }

    public CounterStat getGenericFailureStat(Operation operation)
    {
        return (operation == Operation.FetchStats) ? fetchStatGenericFailure : putStatGenericFailure;
    }

    public CounterStat getTimeoutFailureStat(Operation operation)
    {
        return (operation == Operation.FetchStats) ? fetchStatTimeoutFailure : putStatTimeoutFailure;
    }

    public CounterStat getRedisFailureStat(Operation operation)
    {
        return (operation == Operation.FetchStats) ? fetchStatRedisFailure : putStatRedisFailure;
    }

    public <V> V execute(Callable<V> callable, Operation operation)
    {
        try {
            return wrap(callable, operation).call();
        }
        catch (TimeoutException e) {
            getTimeoutFailureStat(operation).update(1);
            log.error(String.format("Redis Stat Provider Error:" +
                    " Request %s failed with Timeout %s", operation, (e.getMessage() == null) ? "" : e.getMessage()));
        }
        catch (RedisException e) {
            getRedisFailureStat(operation).update(1);
            log.error(String.format("Redis Stat Provider Error:" +
                    " Request %s failed with RedisException %s", operation, (e.getMessage() == null) ? "" : e.getMessage()));
        }
        catch (Exception e) {
            getGenericFailureStat(operation).update(1);
            log.error(String.format("Redis Stat Provider Error:" +
                    " Request %s failed with GenericException %s", operation, (e.getMessage() == null) ? "" : e.getMessage()));
        }
        return null;
    }

    public <V> Callable<V> wrap(Callable<V> callable, Operation operation)
    {
        return () -> {
            try (TimeStat.BlockTimer ignored = getTime(operation).time()) {
                getStatRequest(operation).update(1);
                return callable.call();
            }
            catch (Exception e) {
                // Will be caught outside
                throw e;
            }
        };
    }

    @Managed
    @Nested
    public CounterStat getPutStatTimeoutFailure()
    {
        return putStatTimeoutFailure;
    }

    @Managed
    @Nested
    public CounterStat getPutStatGenericFailure()
    {
        return putStatGenericFailure;
    }

    @Managed
    @Nested
    public CounterStat getPutStatRequest()
    {
        return putStatRequest;
    }

    @Managed
    @Nested
    public CounterStat getPutStatRedisFailure()
    {
        return putStatRedisFailure;
    }

    @Managed
    @Nested
    public CounterStat getFetchStatRedisFailure()
    {
        return fetchStatRedisFailure;
    }

    @Managed
    @Nested
    public CounterStat getFetchStatRequest()
    {
        return fetchStatRequest;
    }

    @Managed
    @Nested
    public CounterStat getFetchStatGenericFailure()
    {
        return fetchStatGenericFailure;
    }

    @Managed
    @Nested
    public CounterStat getFetchStatTimeoutFailure()
    {
        return fetchStatTimeoutFailure;
    }

    @Managed
    @Nested
    public TimeStat getFetchTime()
    {
        return fetchTime;
    }

    @Managed
    @Nested
    public TimeStat getPutTime()
    {
        return putTime;
    }
}
