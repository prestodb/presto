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
package com.facebook.presto.connector.system;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.sql.SqlPath;
import com.facebook.presto.transaction.TransactionId;

import java.util.Optional;

public final class SystemConnectorSessionUtil
{
    private static final SystemSessionProperties SYSTEM_SESSION_PROPERTIES = new SystemSessionProperties();

    private SystemConnectorSessionUtil() {}

    // this does not preserve any connector properties (for the system connector)
    public static Session toSession(ConnectorTransactionHandle transactionHandle, ConnectorSession session)
    {
        TransactionId transactionId = ((GlobalSystemTransactionHandle) transactionHandle).getTransactionId();
        return Session.builder(new SessionPropertyManager(SYSTEM_SESSION_PROPERTIES))
                .setQueryId(new QueryId(session.getQueryId()))
                .setTransactionId(transactionId)
                .setCatalog("catalog")
                .setSchema("schema")
                .setPath(new SqlPath(Optional.of("path")))
                .setIdentity(session.getIdentity())
                .setTimeZoneKey(session.getTimeZoneKey())
                .setLocale(session.getLocale())
                .setStartTime(session.getStartTime())
                .build();
    }
}
