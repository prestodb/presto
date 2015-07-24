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
package com.facebook.presto.verifier;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class Query
{
    private final String catalog;
    private final String schema;
    private final String query;
    private final String username;
    private final String password;
    private final Map<String, String> sessionProperties;

    public Query(String catalog, String schema, String query, String username, String password, Map<String, String> sessionProperties)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.query = clean(query);
        this.username = username;
        this.password = password;
        this.sessionProperties = ImmutableMap.copyOf(sessionProperties);
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getQuery()
    {
        return query;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    private static String clean(String sql)
    {
        sql = sql.replaceAll("\t", "  ");
        sql = sql.replaceAll("\n+", "\n");
        sql = sql.trim();
        while (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }
}
