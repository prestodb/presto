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
package com.facebook.presto;

import com.facebook.presto.mcp.PrestoQueryClient;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public class DummyPrestoQueryClient
        extends PrestoQueryClient
{
    private final Map<String, List<List<Object>>> responses;

    public DummyPrestoQueryClient(Map<String, List<List<Object>>> responses)
    {
        super(null);
        this.responses = responses;
    }

    @Override
    public List<List<Object>> runQuery(String sql, String token)
    {
        return responses.getOrDefault(sql, ImmutableList.of());
    }

    @Override
    public String applyLimit(String sql)
    {
        if (!sql.toLowerCase().contains("limit")) {
            return sql + " LIMIT 1000";
        }
        return sql;
    }
}
