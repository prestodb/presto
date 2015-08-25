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

import com.facebook.presto.execution.SystemMemoryUsageListener;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SystemMemoryUpdater
        implements SystemMemoryUsageListener
{
    private final OperatorContext operatorContext;

    public SystemMemoryUpdater(OperatorContext operatorContext)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    }

    @Override
    public void updateSystemMemoryUsage(long deltaMemoryInBytes)
    {
        if (deltaMemoryInBytes > 0) {
            operatorContext.reserveSystemMemory(deltaMemoryInBytes);
        }
        else {
            operatorContext.freeSystemMemory(-deltaMemoryInBytes);
        }
    }

    public void reserveSystemMemroryUsage(long memoryBytesToReserve)
    {
        checkArgument(memoryBytesToReserve >= 0, "memoryBytesToReserve is negative");
        operatorContext.reserveSystemMemory(memoryBytesToReserve);
    }

    public void freeSystemMemroryUsage(long memoryBytesToFree)
    {
        checkArgument(memoryBytesToFree >= 0, "memoryBytesToFree is negative");
        operatorContext.freeSystemMemory(memoryBytesToFree);
    }
}
