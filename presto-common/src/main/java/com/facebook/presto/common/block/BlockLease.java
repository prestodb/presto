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
package com.facebook.presto.common.block;

import java.util.function.Supplier;

/**
 * Represents a Block backed by a closeable resource.  The Block is retrieved via the {@link #get()}
 * method, which must only be called once.  The {@link #close()} method must be called unconditionally
 * when use of the Block is complete.
 */
public interface BlockLease
        extends Supplier<Block>, AutoCloseable
{
    void close();
}
