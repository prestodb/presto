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
package com.facebook.presto.spi;

import com.facebook.presto.spi.block.Block;
import io.airlift.slice.Slice;

import java.util.Collection;

public interface ConnectorPageSink
{
    void appendPage(Page page, Block sampleWeightBlock);

    Collection<Slice> commit();

    /**
     * Get the memory that needs to be reserved from the system pool
     * @return the system memory used so far in table write
     */
    long getDeltaMemory();

    void rollback();
}
