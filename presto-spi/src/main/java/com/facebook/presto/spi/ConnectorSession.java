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

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ConnectorSession
{
    String getQueryId();

    Optional<String> getSource();

    default String getUser()
    {
        return getIdentity().getUser();
    }

    ConnectorIdentity getIdentity();

    Locale getLocale();

    Optional<String> getTraceToken();

    Optional<String> getClientInfo();

    Set<String> getClientTags();

    long getStartTime();

    SqlFunctionProperties getSqlFunctionProperties();

    Map<SqlFunctionId, SqlInvokedFunction> getSessionFunctions();

    <T> T getProperty(String name, Class<T> type);

    Optional<String> getSchema();
}
