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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;

import java.io.IOException;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSPageSource
implements ConnectorPageSource
{
    /**
     * Gets the total input bytes that will be processed by this page source.
     * This is normally the same size as the split.  If size is not available,
     * this method should return zero.
     */
    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    /**
     * Gets the number of input bytes processed by this page source so far.
     * If size is not available, this method should return zero.
     */
    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    /**
     * Gets the wall time this page source spent reading data from the input.
     * If read time is not available, this method should return zero.
     */
    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    /**
     * Will this page source product more pages?
     */
    @Override
    public boolean isFinished()
    {
        return false;
    }

    /**
     * Gets the next page of data.  This method is allowed to return null.
     */
    @Override
    public Page getNextPage()
    {
        return null;
    }

    /**
     * Get the total memory that needs to be reserved in the system memory pool.
     * This memory should include any buffers, etc. that are used for reading data.
     *
     * @return the system memory used so far in table read
     */
    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    /**
     * Immediately finishes this page source.  Presto will always call this method.
     */
    @Override
    public void close() throws IOException
    {
        return;
    }
}
