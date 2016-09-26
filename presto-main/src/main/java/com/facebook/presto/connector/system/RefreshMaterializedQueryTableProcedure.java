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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.execution.MaterializedQueryTableRefresher;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class RefreshMaterializedQueryTableProcedure
{
    private final MaterializedQueryTableRefresher tableRefresher;
    private final JsonCodec<Map<String, String>> jsonCodec;

    @Inject
    public RefreshMaterializedQueryTableProcedure(MaterializedQueryTableRefresher tableRefresher, JsonCodec<Map<String, String>> jsonCodec)
    {
        this.tableRefresher = requireNonNull(tableRefresher, "tableRefresher is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
    }

    @UsedByGeneratedCode
    public void refresh(String materializedQueryTable, String predicateForBaseTables, String predicateForMaterializedQueryTable, ConnectorSession session) throws Exception
    {
        tableRefresher.refreshMaterializedQueryTable(materializedQueryTable, parsePredicates(predicateForBaseTables), predicateForMaterializedQueryTable, session);
    }

    private Map<String, String> parsePredicates(String predicate)
    {
        if (nullToEmpty(predicate).trim().isEmpty()) {
            return emptyMap();
        }

        return jsonCodec.fromJson(predicate);
    }

    public Procedure getProcedure()
    {
        return new Procedure(
                "mqt",
                "admin_refresh",
                ImmutableList.of(
                        new Procedure.Argument("materialized_query_table", VARCHAR),
                        new Procedure.Argument("predicate_for_base_tables", VARCHAR),
                        new Procedure.Argument("predicate_for_materialized_query_table", VARCHAR)),
                methodHandle(getClass(), "refresh", String.class, String.class, String.class, ConnectorSession.class).bindTo(this));
    }
}
