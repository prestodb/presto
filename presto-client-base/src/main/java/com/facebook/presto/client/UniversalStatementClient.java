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
package com.facebook.presto.client;

import java.io.Closeable;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class UniversalStatementClient implements Closeable
{
    protected final QueryHttpClient client;
    protected final boolean debug;
    protected final String query;
    protected final AtomicReference<QueryResults> currentResults = new AtomicReference<QueryResults>();
    protected final AtomicBoolean gone = new AtomicBoolean();
    protected final AtomicBoolean valid = new AtomicBoolean(true);
    protected final String timeZoneId;

    public UniversalStatementClient(QueryHttpClient client,
                                    ClientSession session,
                                    String query) throws RuntimeException
    {
        checkNotNull(client, "client is null");
        checkNotNull(session, "session is null");
        checkNotNull(query, "query is null");

        this.client = client;
        this.debug = session.isDebug();
        this.query = query;
        this.timeZoneId = session.getTimeZoneId();

        try {
            startQuery(session);
        }
        catch (RuntimeException e) {
            gone.set(true);
            throw e;
        }
    }

    public String getQuery()
    {
        return query;
    }

    public String getTimeZoneId()
    {
        return timeZoneId;
    }

    public boolean isClosed()
    {
        return client.isClosed();
    }

    public boolean isGone()
    {
        return gone.get();
    }

    public boolean isDebug()
    {
        return debug;
    }

    public boolean isFailed()
    {
        return currentResults.get().getError() != null;
    }

    public QueryResults current()
    {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return currentResults.get();
    }

    public QueryResults finalResults()
    {
        checkState((!isValid()) || isFailed(), "current position is still valid");
        return currentResults.get();
    }

    public boolean isValid()
    {
        return valid.get() && (!isGone()) && (!isClosed());
    }

    public boolean advance() throws RuntimeException
    {
        if (isClosed() || (current().getNextUri() == null)) {
            valid.set(false);
            return false;
        }

        try {
            URI uri = current().getNextUri();
            currentResults.set(client.execute(uri));
        }
        catch (RuntimeException e) {
            gone.set(true);
            throw e;
        }
        return true;
    }

    @Override
    public void close()
    {
        if (client.isClosed()) {
            return;
        }
        client.close();
        URI uri = currentResults.get().getNextUri();
        if (uri != null) {
            client.deleteAsync(uri);
        }
    }

    public boolean cancelLeafStage()
    {
        checkState(!isClosed(), "client is closed");

        URI uri = current().getPartialCancelUri();
        if (uri == null) {
            return false;
        }

        return client.delete(uri);
    }

    public Map<String, String> getSetSessionProperties()
    {
        return client.getSetSessionProperties();
    }

    public Set<String> getResetSessionProperties()
    {
        return client.getResetSessionProperties();
    }

    protected void startQuery(ClientSession session)
    {
        currentResults.set(client.startQuery(session, query));
    }
}
