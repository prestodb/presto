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

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
public interface SingleStreamSpiller
        extends Closeable
{
    /**
     * Initiate spilling of pages stream. Returns completed future once spilling has finished.
     */
    CompletableFuture<?> spill(Iterator<Page> page);

    /**
     * Initiate spilling of single page. Returns completed future once spilling has finished.
     */
    CompletableFuture<?> spill(Page page);

    /**
     * Returns list of previously spilled Pages as a single stream. Pages are in the same order
     * as they were spilled as long as new spill requests are scheduled after previous request
     * completed. In other words, when calling spill methods simultaneously order of the execution
     * is not guaranteed.
     */
    Iterator<Page> getSpilledPages();

    /**
     * Close releases/removes all underlying resources used during spilling
     * like for example all created temporary files.
     */
    @Override
    void close();
}
