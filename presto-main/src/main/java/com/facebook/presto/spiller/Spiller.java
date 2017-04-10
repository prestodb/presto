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

import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

public interface Spiller
        extends Closeable
{
    /**
     * Initiate spilling of pages stream. Returns completed future once spilling has finished.
     */
    ListenableFuture<?> spill(Iterator<Page> pageIterator);

    /**
     * Returns list of previously spilled Pages streams.
     */
    List<Iterator<Page>> getSpills();

    /**
     * Close releases/removes all underlying resources used during spilling
     * like for example all created temporary files.
     */
    @Override
    void close();
}
