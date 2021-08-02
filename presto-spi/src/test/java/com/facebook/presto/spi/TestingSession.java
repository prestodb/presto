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

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static java.util.Locale.ENGLISH;

public final class TestingSession
{
    public static final ConnectorSession SESSION = new ConnectorSession()
    {
        @Override
        public String getQueryId()
        {
            return "test_query_id";
        }

        @Override
        public Optional<String> getSource()
        {
            return Optional.of("TestSource");
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return new ConnectorIdentity("user", Optional.empty(), Optional.empty());
        }

        @Override
        public Optional<String> getClientInfo()
        {
            return Optional.of("TestClientInfo");
        }

        @Override
        public Set<String> getClientTags()
        {
            return ImmutableSet.of();
        }

        @Override
        public Locale getLocale()
        {
            return ENGLISH;
        }

        @Override
        public long getStartTime()
        {
            return 0;
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return Optional.empty();
        }

        @Override
        public SqlFunctionProperties getSqlFunctionProperties()
        {
            return SqlFunctionProperties.builder()
                    .setTimeZoneKey(UTC_KEY)
                    .setLegacyTimestamp(true)
                    .setSessionStartTime(getStartTime())
                    .setSessionLocale(getLocale())
                    .setSessionUser(getUser())
                    .build();
        }

        @Override
        public Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions()
        {
            return ImmutableMap.of();
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "Unknown session property " + name);
        }

        @Override
        public Optional<String> getSchema()
        {
            return Optional.empty();
        }
    };

    private TestingSession() {}
}
