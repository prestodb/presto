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
package com.facebook.presto.operator;

import com.facebook.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class ExchangeClientStats
{
    private int shuttingDownClientCount;
    private int pendingShuttingDownClientCount;
    private final TimeStat fastDrainingTime = new TimeStat(MILLISECONDS);

    public void setShuttingDownClientCount(int shuttingDownClientCount)
    {
        this.shuttingDownClientCount = shuttingDownClientCount;
    }

    public void setPendingShuttingDownClientCount(int pendingShuttingDownClientCount)
    {
        this.pendingShuttingDownClientCount = pendingShuttingDownClientCount;
    }

    @Managed
    @Nested
    public TimeStat getFastDrainingTime()
    {
        return fastDrainingTime;
    }

    @Managed
    @Nested
    public int getShuttingDownClientCount()
    {
        return shuttingDownClientCount;
    }

    @Managed
    @Nested
    public int getPendingShuttingDownClientCount()
    {
        return pendingShuttingDownClientCount;
    }
}
