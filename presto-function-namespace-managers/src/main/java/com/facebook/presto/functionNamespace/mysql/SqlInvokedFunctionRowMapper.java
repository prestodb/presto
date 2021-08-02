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
package com.facebook.presto.functionNamespace.mysql;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.function.FunctionVersion.withVersion;

public class SqlInvokedFunctionRowMapper
        implements RowMapper<SqlInvokedFunction>
{
    private static final JsonCodec<List<Parameter>> PARAMETERS_CODEC = listJsonCodec(Parameter.class);
    private static final JsonCodec<RoutineCharacteristics> ROUTINE_CHARACTERISTICS_CODEC = jsonCodec(RoutineCharacteristics.class);

    @Override
    public SqlInvokedFunction map(ResultSet rs, StatementContext ctx)
            throws SQLException
    {
        String catalog = rs.getString("catalog_name");
        String schema = rs.getString("schema_name");
        String functionName = rs.getString("function_name");
        List<Parameter> parameters = PARAMETERS_CODEC.fromJson(rs.getString("parameters"));
        String returnType = rs.getString("return_type");
        String description = rs.getString("description");
        RoutineCharacteristics routineCharacteristics = ROUTINE_CHARACTERISTICS_CODEC.fromJson(rs.getString("routine_characteristics"));
        String body = rs.getString("body");
        String version = String.valueOf(rs.getLong("version"));

        return new SqlInvokedFunction(
                QualifiedObjectName.valueOf(catalog, schema, functionName),
                parameters,
                parseTypeSignature(returnType),
                description,
                routineCharacteristics,
                body,
                withVersion(version));
    }
}
