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
package com.facebook.presto.spiller;

import org.weakref.jmx.Managed;

import java.util.concurrent.atomic.AtomicLong;

public class SpillerStats
{
    protected final AtomicLong totalSpilledBytes = new AtomicLong();

    @Managed
    public long getTotalSpilledBytes()
    {
        return totalSpilledBytes.get();
    }

    public void addToTotalSpilledBytes(long delta)
    {
        totalSpilledBytes.addAndGet(delta);
    }
}
