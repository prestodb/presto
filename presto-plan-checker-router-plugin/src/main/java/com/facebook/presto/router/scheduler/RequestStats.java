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

package com.facebook.presto.router.scheduler;

import com.facebook.airlift.stats.CounterStat;

public class RequestStats
        implements RequestStatsMBean
{
    private final CounterStat javaClusterRedirectRequests = new CounterStat();
    private final CounterStat nativeClusterRedirectRequests = new CounterStat();
    private final CounterStat fallBackToJavaClusterRedirectRequests = new CounterStat();

    // --- Updaters ---
    public void updateJavaRequests(long count)
    {
        javaClusterRedirectRequests.update(count);
    }

    public void updateNativeRequests(long count)
    {
        nativeClusterRedirectRequests.update(count);
    }

    public void updateFallBackToJavaClusterRequests(long count)
    {
        fallBackToJavaClusterRedirectRequests.update(count);
    }

    // --- Java Cluster Metrics ---
    @Override
    public double getJavaClusterRedirectRequestsOneMinuteCount()
    {
        return javaClusterRedirectRequests.getOneMinute().getCount();
    }

    @Override
    public double getJavaClusterRedirectRequestsOneMinuteRate()
    {
        return javaClusterRedirectRequests.getFiveMinute().getRate();
    }

    @Override
    public double getJavaClusterRedirectRequestsFiveMinuteCount()
    {
        return javaClusterRedirectRequests.getFiveMinute().getCount();
    }

    @Override
    public double getJavaClusterRedirectRequestsFiveMinuteRate()
    {
        return javaClusterRedirectRequests.getFiveMinute().getRate();
    }

    @Override
    public double getJavaClusterRedirectRequestsFifteenMinuteCount()
    {
        return javaClusterRedirectRequests.getFifteenMinute().getCount();
    }

    @Override
    public double getJavaClusterRedirectRequestsFifteenMinuteRate()
    {
        return javaClusterRedirectRequests.getFifteenMinute().getRate();
    }

    @Override
    public long getJavaClusterRedirectRequestsTotalCount()
    {
        return javaClusterRedirectRequests.getTotalCount();
    }

    // --- Native Cluster Metrics ---
    @Override
    public double getNativeClusterRedirectRequestsOneMinuteCount()
    {
        return nativeClusterRedirectRequests.getOneMinute().getCount();
    }

    @Override
    public double getNativeClusterRedirectRequestsOneMinuteRate()
    {
        return nativeClusterRedirectRequests.getOneMinute().getRate();
    }

    @Override
    public double getNativeClusterRedirectRequestsFiveMinuteCount()
    {
        return nativeClusterRedirectRequests.getFiveMinute().getCount();
    }

    @Override
    public double getNativeClusterRedirectRequestsFiveMinuteRate()
    {
        return nativeClusterRedirectRequests.getFiveMinute().getRate();
    }

    @Override
    public double getNativeClusterRedirectRequestsFifteenMinuteCount()
    {
        return nativeClusterRedirectRequests.getFifteenMinute().getCount();
    }

    @Override
    public double getNativeClusterRedirectRequestsFifteenMinuteRate()
    {
        return nativeClusterRedirectRequests.getFifteenMinute().getRate();
    }

    @Override
    public long getNativeClusterRedirectRequestsTotalCount()
    {
        return nativeClusterRedirectRequests.getTotalCount();
    }

    // --- Fall back to Java Cluster Metrics ---
    @Override
    public double getFallbackToJavaClusterRedirectRequestsOneMinuteCount()
    {
        return fallBackToJavaClusterRedirectRequests.getOneMinute().getCount();
    }

    @Override
    public double getFallbackToJavaClusterRedirectRequestsOneMinuteRate()
    {
        return fallBackToJavaClusterRedirectRequests.getOneMinute().getRate();
    }

    @Override
    public double getFallbackToJavaClusterRedirectRequestsFiveMinuteCount()
    {
        return fallBackToJavaClusterRedirectRequests.getFiveMinute().getCount();
    }

    @Override
    public double getFallbackToJavaClusterRedirectRequestsFiveMinuteRate()
    {
        return fallBackToJavaClusterRedirectRequests.getFiveMinute().getRate();
    }

    @Override
    public double getFallbackToJavaClusterRedirectRequestsFifteenMinuteCount()
    {
        return fallBackToJavaClusterRedirectRequests.getFifteenMinute().getCount();
    }

    @Override
    public double getFallbackToJavaClusterRedirectRequestsFifteenMinuteRate()
    {
        return fallBackToJavaClusterRedirectRequests.getFifteenMinute().getRate();
    }

    @Override
    public long getFallbackToJavaClusterRedirectRequestsTotalCount()
    {
        return fallBackToJavaClusterRedirectRequests.getTotalCount();
    }
}
