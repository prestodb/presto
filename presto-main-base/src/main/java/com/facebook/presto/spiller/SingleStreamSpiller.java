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
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

import static com.google.common.collect.Iterators.singletonIterator;

public interface SingleStreamSpiller
        extends Closeable
{
    /**
     * Initiate spilling of pages stream. Returns completed future once spilling has finished.
     * Next spill can be initiated as soon as previous one completes.
     */
    ListenableFuture<?> spill(Iterator<Page> page);

    /**
     * Initiate spilling of single page. Returns completed future once spilling has finished.
     * Next spill can be initiated as soon as previous one completes.
     */
    default ListenableFuture<?> spill(Page page)
    {
        return spill(singletonIterator(page));
    }

    /**
     * Returns list of previously spilled Pages as a single stream. Commits the spill file automatically
     * if not committed. Pages are in the same order as they were spilled. Method requires the
     * issued spill request to be completed.
     */
    Iterator<Page> getSpilledPages();

    /**
     * Returns estimate size of pages that would be returned by {@link #getAllSpilledPages()}.
     */
    long getSpilledPagesInMemorySize();

    /**
     * Initiates read of previously spilled pages. Commits the spill file automatically if not committed
     * The returned {@link Future} will be complete once all pages are read.
     */
    ListenableFuture<List<Page>> getAllSpilledPages();

    /**
     * Commit the spill file. Once committed, the spill file can no longer be modified
     * If the spill file is already committed, invoking this method has no effect
     */
    void commit();

    /**
     * Close releases/removes all underlying resources used during spilling
     * like for example all created temporary files.
     */
    @Override
    void close();
}
