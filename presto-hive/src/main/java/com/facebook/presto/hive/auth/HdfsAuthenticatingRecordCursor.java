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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

public class HdfsAuthenticatingRecordCursor
        implements RecordCursor
{
    private final ConnectorSession connectorSession;
    private final HadoopAuthentication authentication;
    private final RecordCursor targetCursor;

    public HdfsAuthenticatingRecordCursor(ConnectorSession connectorSession, HadoopAuthentication authentication, RecordCursor targetCursor)
    {
        this.connectorSession = connectorSession;
        this.authentication = authentication;
        this.targetCursor = targetCursor;
    }

    @Override
    public long getTotalBytes()
    {
        return authentication.doAs(connectorSession.getUser(), targetCursor::getTotalBytes);
    }

    @Override
    public long getCompletedBytes()
    {
        return authentication.doAs(connectorSession.getUser(), targetCursor::getCompletedBytes);
    }

    @Override
    public long getReadTimeNanos()
    {
        return authentication.doAs(connectorSession.getUser(), targetCursor::getReadTimeNanos);
    }

    @Override
    public Type getType(int field)
    {
        return authentication.doAs(connectorSession.getUser(), () -> targetCursor.getType(field));
    }

    @Override
    public boolean advanceNextPosition()
    {
        return authentication.doAs(connectorSession.getUser(), targetCursor::advanceNextPosition);
    }

    @Override
    public boolean getBoolean(int field)
    {
        return authentication.doAs(connectorSession.getUser(), () -> targetCursor.getBoolean(field));
    }

    @Override
    public long getLong(int field)
    {
        return authentication.doAs(connectorSession.getUser(), () -> targetCursor.getLong(field));
    }

    @Override
    public double getDouble(int field)
    {
        return authentication.doAs(connectorSession.getUser(), () -> targetCursor.getDouble(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        return authentication.doAs(connectorSession.getUser(), () -> targetCursor.getSlice(field));
    }

    @Override
    public Object getObject(int field)
    {
        return authentication.doAs(connectorSession.getUser(), () -> targetCursor.getObject(field));
    }

    @Override
    public boolean isNull(int field)
    {
        return authentication.doAs(connectorSession.getUser(), () -> targetCursor.isNull(field));
    }

    @Override
    public void close()
    {
        authentication.doAs(connectorSession.getUser(), targetCursor::close);
    }
}
