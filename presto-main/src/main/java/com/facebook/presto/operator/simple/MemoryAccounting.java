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
package com.facebook.presto.operator.simple;

import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.simple.SimpleOperator.ProcessorState;
import com.facebook.presto.spi.Page;

import java.util.function.LongSupplier;

public final class MemoryAccounting
{
    private MemoryAccounting()
    {
    }

    public static <T> ProcessorInput<T> wrap(OperatorContext operatorContext, ProcessorInput<T> processorInput, LongSupplier memorySizeSupplier)
    {
        return input -> {
            ProcessorState state = processorInput.addInput(input);
            updateMemory(operatorContext, memorySizeSupplier);
            return state;
        };
    }

    public static ProcessorPageOutput wrap(OperatorContext operatorContext, ProcessorPageOutput operatorOutput, LongSupplier memorySizeSupplier)
    {
        return () -> {
            Page page = operatorOutput.extractOutput();
            updateMemory(operatorContext, memorySizeSupplier);
            return page;
        };
    }

    private static void updateMemory(OperatorContext operatorContext, LongSupplier memorySizeSupplier)
    {
        long memorySize = memorySizeSupplier.getAsLong();
        long preallocated = operatorContext.getOperatorPreAllocatedMemory().toBytes();
        operatorContext.setMemoryReservation(Math.max(0, memorySize - preallocated));
    }
}
