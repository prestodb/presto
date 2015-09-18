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
import com.facebook.presto.memory.LocalMemoryContext;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SystemMemoryUsageTracker
        implements SystemMemoryUsageListener
{
    // This implementation is a hack.
    // DO NOT use MemoryContext in a way similar to how it is used here.

    private final LocalMemoryContext memoryContext;
    private long bytes;

    public SystemMemoryUsageTracker(OperatorContext operatorContext)
    {
        this.memoryContext = requireNonNull(operatorContext, "operatorContext is null").getSystemMemoryContext().newLocalMemoryContext();
    }

    @Override
    public void updateSystemMemoryUsage(long deltaMemoryInBytes)
    {
        checkArgument(bytes + deltaMemoryInBytes >= 0, "tried to free more memory than is reserved");
        bytes += deltaMemoryInBytes;
        memoryContext.setBytes(bytes);
    }
}
