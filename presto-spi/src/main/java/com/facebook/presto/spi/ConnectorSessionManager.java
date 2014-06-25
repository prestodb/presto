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

package com.facebook.presto.spi;

import java.util.Locale;
import java.util.Map;
import java.util.HashMap;

import com.facebook.presto.spi.type.TimeZoneKey;

public class ConnectorSessionManager
{
    private final Map<String, ConnectorSession> connectorSessionMap;

    public ConnectorSessionManager()
    {
        this.connectorSessionMap = new HashMap<String, ConnectorSession>();
    }

    public ConnectorSession getSession(String sessionId)
    {
        if (this.connectorSessionMap.containsKey(sessionId)) {
            return connectorSessionMap.get(sessionId);
        }
        return null;
    }

    public ConnectorSession createOrUpdateSession(String user, String source, String catalog, String schema, TimeZoneKey timeZoneKey, Locale locale, String remoteUserAddress, String userAgent, String sessionId)
    {
        if (sessionId == null) {
            return new ConnectorSession(user, source, catalog, schema, timeZoneKey, locale, remoteUserAddress, userAgent);
        }
        else {
            Map<String, Object> configs = new HashMap<String, Object>();
            if (getSession(sessionId) != null) {
                configs = getSession(sessionId).getConfigs();
            }
            ConnectorSession session = new ConnectorSession(user, source, catalog, schema, timeZoneKey, locale, remoteUserAddress, userAgent, sessionId, configs);
            this.connectorSessionMap.put(sessionId, session);
            return session;
        }
    }

    public void removeSession(String sessionId)
    {
        if (this.connectorSessionMap.containsKey(sessionId)) {
            connectorSessionMap.remove(sessionId);
        }
    }
}
