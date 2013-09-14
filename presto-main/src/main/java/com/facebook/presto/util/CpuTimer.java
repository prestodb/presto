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
package com.facebook.presto.util;

import com.google.common.base.Objects;
import io.airlift.units.Duration;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class CpuTimer
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final long wallStartTime;
    private final long cpuStartTime;
    private final long userStartTime;

    private long intervalWallStart;
    private long intervalCpuStart;
    private long intervalUserStart;

    public CpuTimer()
    {
        wallStartTime = System.nanoTime();
        cpuStartTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        userStartTime = THREAD_MX_BEAN.getCurrentThreadUserTime();

        intervalWallStart = wallStartTime;
        intervalCpuStart = cpuStartTime;
        intervalUserStart = userStartTime;
    }

    public CpuDuration startNewInterval()
    {
        long currentWallTime = System.nanoTime();
        long currentCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        long currentUserTime = THREAD_MX_BEAN.getCurrentThreadUserTime();

        CpuDuration cpuDuration = new CpuDuration(
                nanosBetween(intervalWallStart, currentWallTime),
                nanosBetween(intervalCpuStart, currentCpuTime),
                nanosBetween(intervalUserStart, currentUserTime));

        intervalWallStart = currentWallTime;
        intervalCpuStart = currentCpuTime;
        intervalUserStart = currentUserTime;

        return cpuDuration;
    }

    public CpuDuration elapsedIntervalTime()
    {
        long currentWallTime = System.nanoTime();
        long currentCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        long currentUserTime = THREAD_MX_BEAN.getCurrentThreadUserTime();

        return new CpuDuration(
                nanosBetween(intervalWallStart, currentWallTime),
                nanosBetween(intervalCpuStart, currentCpuTime),
                nanosBetween(intervalUserStart, currentUserTime));
    }

    public CpuDuration elapsedTime()
    {
        long currentWallTime = System.nanoTime();
        long currentCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        long currentUserTime = THREAD_MX_BEAN.getCurrentThreadUserTime();

        return new CpuDuration(
                nanosBetween(wallStartTime, currentWallTime),
                nanosBetween(cpuStartTime, currentCpuTime),
                nanosBetween(userStartTime, currentUserTime));
    }

    private Duration nanosBetween(long start, long end)
    {
        return new Duration(Math.abs(end - start), NANOSECONDS);
    }

    public static class CpuDuration
    {
        private final Duration wall;
        private final Duration cpu;
        private final Duration user;

        public CpuDuration()
        {
            this.wall = new Duration(0, NANOSECONDS);
            this.cpu = new Duration(0, NANOSECONDS);
            this.user = new Duration(0, NANOSECONDS);
        }

        public CpuDuration(Duration wall, Duration cpu, Duration user)
        {
            this.wall = wall;
            this.cpu = cpu;
            this.user = user;
        }

        public Duration getWall()
        {
            return wall;
        }

        public Duration getCpu()
        {
            return cpu;
        }

        public Duration getUser()
        {
            return user;
        }

        public CpuDuration add(CpuDuration cpuDuration)
        {
            return new CpuDuration(
                    addDurations(wall, cpuDuration.wall),
                    addDurations(cpu, cpuDuration.cpu),
                    addDurations(user, cpuDuration.user));
        }

        public CpuDuration subtract(CpuDuration cpuDuration)
        {
            return new CpuDuration(
                    subtractDurations(wall, cpuDuration.wall),
                    subtractDurations(cpu, cpuDuration.cpu),
                    subtractDurations(user, cpuDuration.user));
        }

        private static Duration addDurations(Duration a, Duration b)
        {
            return new Duration(a.getValue(NANOSECONDS) + b.getValue(NANOSECONDS), NANOSECONDS);
        }

        private static Duration subtractDurations(Duration a, Duration b)
        {
            return new Duration(Math.max(0, a.getValue(NANOSECONDS) - b.getValue(NANOSECONDS)), NANOSECONDS);
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("wall", wall)
                    .add("cpu", cpu)
                    .add("user", user)
                    .toString();
        }
    }
}
