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
package com.facebook.presto.sql.planner.optimizations;

import org.weakref.jmx.Managed;

import java.util.concurrent.atomic.AtomicLong;

public class MVOptimizerStats
{
    private final AtomicLong hit = new AtomicLong();
    private final AtomicLong miss = new AtomicLong();

    public void incrementMVHit()
    {
        hit.getAndIncrement();
    }

    public void incrementMVMiss()
    {
        miss.getAndIncrement();
    }

    @Managed
    public long getMVHit()
    {
        return hit.get();
    }

    @Managed
    public long getMVMiss()
    {
        return miss.get();
    }
}
