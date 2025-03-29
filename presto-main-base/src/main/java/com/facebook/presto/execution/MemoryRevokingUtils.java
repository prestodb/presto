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
package com.facebook.presto.execution;

import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryPool;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MemoryRevokingUtils
{
    private MemoryRevokingUtils() {}

    public static List<MemoryPool> getMemoryPools(LocalMemoryManager localMemoryManager)
    {
        requireNonNull(localMemoryManager, "localMemoryManager can not be null");
        ImmutableList.Builder<MemoryPool> builder = new ImmutableList.Builder<>();
        builder.add(localMemoryManager.getGeneralPool());
        localMemoryManager.getReservedPool().ifPresent(builder::add);
        return builder.build();
    }
}
