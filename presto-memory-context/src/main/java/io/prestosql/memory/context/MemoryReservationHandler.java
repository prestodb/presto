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
package io.prestosql.memory.context;

import com.google.common.util.concurrent.ListenableFuture;

public interface MemoryReservationHandler
{
    /**
     * @return a future that signals the caller to block before reserving more memory.
     */
    ListenableFuture<?> reserveMemory(String allocationTag, long delta);

    /**
     * Try reserving the given number of bytes.
     *
     * @return true if reservation is successful, false otherwise.
     */
    boolean tryReserveMemory(String allocationTag, long delta);
}
