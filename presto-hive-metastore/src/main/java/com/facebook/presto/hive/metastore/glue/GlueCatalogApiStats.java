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
package com.facebook.presto.hive.metastore.glue;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class GlueCatalogApiStats
{
    private final TimeStat time = new TimeStat(MILLISECONDS);
    private final CounterStat totalFailures = new CounterStat();

    public <T> T record(Supplier<T> action)
    {
        try (TimeStat.BlockTimer timer = time.time()) {
            return action.get();
        }
        catch (Exception e) {
            recordException(e);
            throw e;
        }
    }

    public void record(Runnable action)
    {
        try (TimeStat.BlockTimer timer = time.time()) {
            action.run();
        }
        catch (Exception e) {
            recordException(e);
            throw e;
        }
    }

    public <R extends AmazonWebServiceRequest, T> AsyncHandler<R, T> metricsAsyncHandler()
    {
        return new AsyncHandler<R, T>() {
            private final TimeStat.BlockTimer timer = time.time();
            @Override
            public void onError(Exception exception)
            {
                timer.close();
                recordException(exception);
            }

            @Override
            public void onSuccess(R request, T result)
            {
                timer.close();
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

    private void recordException(Exception e)
    {
        totalFailures.update(1);
    }
}
