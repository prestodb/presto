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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

public class Query
{
    private final String catalog;
    private final String schema;
    private final List<String> preQueries;
    private final String query;
    private final List<String> postQueries;
    private final String username;
    private final String password;
    private final Map<String, String> sessionProperties;

    @JsonCreator
    public Query(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("preQueries") List<String> preQueries,
            @JsonProperty("query") String query,
            @JsonProperty("postQueries") List<String> postQueries,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("sessionProperties") Map<String, String> sessionProperties)
    {
        this.catalog = catalog;
        this.schema = schema;
        this.preQueries = preQueries.stream().map(Query::clean).collect(toImmutableList());
        this.query = clean(query);
        this.postQueries = postQueries.stream().map(Query::clean).collect(toImmutableList());
        this.username = username;
        this.password = password;
        this.sessionProperties = ImmutableMap.copyOf(sessionProperties);
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<String> getPreQueries()
    {
        return preQueries;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public List<String> getPostQueries()
    {
        return postQueries;
    }

    @JsonProperty
    public String getUsername()
    {
        return username;
    }

    @JsonProperty
    public String getPassword()
    {
        return password;
    }

    @JsonProperty
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
