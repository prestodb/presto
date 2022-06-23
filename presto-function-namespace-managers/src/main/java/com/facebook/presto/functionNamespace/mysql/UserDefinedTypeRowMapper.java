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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

public class UserDefinedTypeRowMapper
        implements RowMapper<UserDefinedType>
{
    @Override
    public UserDefinedType map(ResultSet rs, StatementContext ctx)
            throws SQLException
    {
        QualifiedObjectName typeName = QualifiedObjectName.valueOf(
                rs.getString("catalog_name"),
                rs.getString("schema_name"),
                rs.getString("type_name"));
        String physicalType = rs.getString("physical_type");
        TypeSignature typeSignature = parseTypeSignature(physicalType);
        return new UserDefinedType(typeName, typeSignature);
    }
}
