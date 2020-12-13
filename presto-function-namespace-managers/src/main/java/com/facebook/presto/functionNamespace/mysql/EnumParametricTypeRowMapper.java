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
import com.facebook.presto.common.type.LongEnumParametricType;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharEnumParametricType;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class EnumParametricTypeRowMapper
        implements RowMapper<ParametricType>
{
    private static final JsonCodec<List<String>> PARAMETERS_CODEC = listJsonCodec(String.class);

    @Override
    public ParametricType map(ResultSet rs, StatementContext ctx)
            throws SQLException
    {
        QualifiedObjectName typeName = QualifiedObjectName.valueOf(
                rs.getString("catalog_name"),
                rs.getString("schema_name"),
                rs.getString("type_name"));
        List<String> typeParameters = PARAMETERS_CODEC.fromJson(rs.getString("type_parameters"));
        checkArgument(typeParameters.size() == 1, "Expect enum type to have 1 parameter");
        TypeSignature typeSignature = parseTypeSignature(format("%s(%s)", typeName, typeParameters.get(0)));
        checkArgument(typeSignature.isEnum(), "Expect enum type");
        if (typeSignature.isLongEnum()) {
            return new LongEnumParametricType(typeName, typeSignature.getParameters().get(0).getLongEnumMap());
        }
        return new VarcharEnumParametricType(typeName, typeSignature.getParameters().get(0).getVarcharEnumMap());
    }
}
