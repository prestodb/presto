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
package io.prestosql.memory;

import java.util.function.Consumer;

public interface MemoryPoolListener
{
    /**
     * Invoked when memory reservation completes successfully.
     *
     * @param memoryPool the {@link MemoryPool} where the reservation took place
     */
    void onMemoryReserved(MemoryPool memoryPool);

    /**
     * Creates {@link MemoryPoolListener} implementing {@link #onMemoryReserved(MemoryPool)} only.
     */
    static MemoryPoolListener onMemoryReserved(Consumer<? super MemoryPool> action)
    {
        return action::accept;
    }
}
