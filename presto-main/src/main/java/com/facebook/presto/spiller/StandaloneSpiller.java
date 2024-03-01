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

import com.facebook.presto.common.Page;
import com.facebook.presto.spi.storage.SerializedStorageHandle;

import java.util.Iterator;

/**
 * StandaloneSpiller is a stateless spiller interface that provides
 * basic spill capabilities using serialized storage handle.
 * This is different from {@link Spiller} interface which is stateful
 * in nature.
 */
public interface StandaloneSpiller
{
    /**
     * Initiate spilling of pages stream. Returns back serialized
     * storage handle which identifies the spill file
     */
    SerializedStorageHandle spill(Iterator<Page> pageIterator);

    /**
     * Returns list of pages present in spill file
     * specified using serialized storage handle
     */
    Iterator<Page> getSpilledPages(SerializedStorageHandle storageHandle);

    /**
     * Remove the spill file using storage handle
     */
    void remove(SerializedStorageHandle storageHandle);
}
