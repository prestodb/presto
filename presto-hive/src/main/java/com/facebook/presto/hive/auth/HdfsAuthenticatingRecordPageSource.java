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
package com.facebook.presto.hive.auth;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;

import java.io.IOException;

public class HdfsAuthenticatingRecordPageSource
        implements RecordPageSource
{
    private final ConnectorSession connectorSession;
    private final HadoopAuthentication authentication;
    private final RecordPageSource targetPageSource;
    private final RecordCursor cursor;

    public HdfsAuthenticatingRecordPageSource(ConnectorSession connectorSession, HadoopAuthentication authentication, RecordPageSource targetPageSource)
    {
        this.connectorSession = connectorSession;
        this.authentication = authentication;
        this.targetPageSource = targetPageSource;
        this.cursor = new HdfsAuthenticatingRecordCursor(connectorSession, authentication, targetPageSource.getCursor());
    }

    @Override
    public RecordCursor getCursor()
    {
        return cursor;
    }

    @Override
    public long getTotalBytes()
    {
        return authentication.doAs(connectorSession.getUser(), targetPageSource::getTotalBytes);
    }

    @Override
    public long getCompletedBytes()
    {
        return authentication.doAs(connectorSession.getUser(), targetPageSource::getCompletedBytes);
    }

    @Override
    public long getReadTimeNanos()
    {
        return authentication.doAs(connectorSession.getUser(), targetPageSource::getReadTimeNanos);
    }

    @Override
    public boolean isFinished()
    {
        return authentication.doAs(connectorSession.getUser(), targetPageSource::isFinished);
    }

    @Override
    public Page getNextPage()
    {
        return authentication.doAs(connectorSession.getUser(), targetPageSource::getNextPage);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return authentication.doAs(connectorSession.getUser(), targetPageSource::getSystemMemoryUsage);
    }

    @Override
    public void close()
            throws IOException
    {
        authentication.doAs(connectorSession.getUser(), targetPageSource::getSystemMemoryUsage);
    }
}
