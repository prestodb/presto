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
package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSPageSink
implements ConnectorPageSink
{
    /**
     * Returns a future that will be completed when the page sink can accept
     * more pages.  If the page sink can accept more pages immediately,
     * this method should return {@code NOT_BLOCKED}.
     *
     * @param page
     */
    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        return null;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return null;
    }

    @Override
    public void abort()
    {
        return;
    }
}
