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
package com.facebook.presto.connector.adapter;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.SafeSplitSource;

import java.util.List;

public class SafeSplitSourceAdapter<S extends ConnectorSplit>
        implements ConnectorSplitSource
{
    private final String dataSourceName;
    private final SafeSplitSource<S> delegate;

    public SafeSplitSourceAdapter(String dataSourceName, SafeSplitSource<S> delegate)
    {
        this.dataSourceName = dataSourceName;
        this.delegate = delegate;
    }

    @Override
    public String getDataSourceName()
    {
        return dataSourceName;
    }

    @Override
    public List<ConnectorSplit> getNextBatch(int maxSize)
            throws InterruptedException
    {
        return (List<ConnectorSplit>) delegate.getNextBatch(maxSize);
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }
}
