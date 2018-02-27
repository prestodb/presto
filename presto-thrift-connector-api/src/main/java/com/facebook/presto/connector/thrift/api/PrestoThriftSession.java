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
package com.facebook.presto.connector.thrift.api;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.Nullable;

import java.util.Map;

import static com.facebook.swift.codec.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftSession
{
    private final String queryId;
    private final String source;
    private final String user;
    private final String principalName;
    private final String zoneId;
    private final String localeCode;
    private final long startTime;
    private final Map<String, PrestoThriftBlock> propertyMap;

    @ThriftConstructor
    public PrestoThriftSession(String queryId,
            @Nullable String source,
            String user,
            @Nullable String principalName,
            String zoneId,
            String localeCode,
            long startTime,
            @Nullable Map<String, PrestoThriftBlock> propertyMap)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.source = source;
        this.user = requireNonNull(user, "user is null");
        this.principalName = principalName;
        this.zoneId = requireNonNull(zoneId, "zoneId is null");
        this.localeCode = requireNonNull(localeCode, "localeCode is null");
        this.startTime = startTime;
        this.propertyMap = propertyMap;
    }

    @ThriftField(1)
    public String getQueryId()
    {
        return queryId;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public String getSource()
    {
        return source;
    }

    @ThriftField(3)
    public String getUser()
    {
        return user;
    }

    @Nullable
    @ThriftField(value = 4, requiredness = OPTIONAL)
    public String getPrincipalName()
    {
        return principalName;
    }

    @ThriftField(5)
    public String getZoneId()
    {
        return zoneId;
    }

    @ThriftField(6)
    public String getLocaleCode()
    {
        return localeCode;
    }

    @ThriftField(7)
    public long getStartTime()
    {
        return startTime;
    }

    @ThriftField(value = 8, requiredness = OPTIONAL)
    public Map<String, PrestoThriftBlock> getPropertyMap()
    {
        return propertyMap;
    }
}
