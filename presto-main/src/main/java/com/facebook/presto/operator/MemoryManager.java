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

import io.airlift.units.DataSize;

public class MemoryManager
{
    private final OperatorContext operatorContext;
    private long currentMemoryReservation;

    public MemoryManager(OperatorContext operatorContext)
    {
        this.operatorContext = operatorContext;
    }

    public boolean canUse(long memorySize)
    {
        // remove the pre-allocated memory from this size
        memorySize -= operatorContext.getOperatorPreAllocatedMemory().toBytes();

        long delta = memorySize - currentMemoryReservation;
        if (delta <= 0) {
            return true;
        }

        if (!operatorContext.reserveMemory(delta)) {
            return false;
        }

        // reservation worked, record the reservation
        currentMemoryReservation = Math.max(currentMemoryReservation, memorySize);
        return true;
    }

    public DataSize getMaxMemorySize()
    {
        return operatorContext.getMaxMemorySize();
    }
}
