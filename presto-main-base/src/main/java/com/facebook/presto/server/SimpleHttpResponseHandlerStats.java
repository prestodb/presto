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
package com.facebook.presto.server;

import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

public class SimpleHttpResponseHandlerStats
{
    private final IncrementalAverage responseSizeBytes = new IncrementalAverage();
    private AtomicLong requestSuccess = new AtomicLong();
    private AtomicLong requestFailure = new AtomicLong();

    public void responseSize(long responseSizeBytes)
    {
        this.responseSizeBytes.add(responseSizeBytes);
    }

    public void updateSuccess()
    {
        requestSuccess.incrementAndGet();
    }

    public void updateFailure()
    {
        requestFailure.incrementAndGet();
    }

    @Managed
    public double getResponseSizeBytes()
    {
        return responseSizeBytes.getAverage();
    }

    @Managed
    public long getRequestSuccess()
    {
        return requestSuccess.get();
    }

    @Managed
    public long getRequestFailure()
    {
        return requestFailure.get();
    }

    @ThreadSafe
    public static class IncrementalAverage
    {
        @GuardedBy("this")
        private volatile long count;
        @GuardedBy("this")
        private volatile double average;

        public synchronized void add(long value)
        {
            count++;
            average = average + (value - average) / count;
        }

        public double getAverage()
        {
            return average;
        }

        public long getCount()
        {
            return count;
        }
    }
}
