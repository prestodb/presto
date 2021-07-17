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

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import org.jdbi.v3.sqlobject.config.RegisterArgumentFactories;
import org.jdbi.v3.sqlobject.config.RegisterArgumentFactory;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.config.RegisterRowMappers;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

@RegisterRowMappers({
        @RegisterRowMapper(SqlInvokedFunctionRowMapper.class),
        @RegisterRowMapper(SqlInvokedFunctionRecordRowMapper.class),
        @RegisterRowMapper(UserDefinedTypeRowMapper.class)
})
@RegisterArgumentFactories({
        @RegisterArgumentFactory(SqlFunctionIdArgumentFactory.class),
        @RegisterArgumentFactory(SqlParametersArgumentFactory.class),
        @RegisterArgumentFactory(TypeSignatureArgumentFactory.class),
        @RegisterArgumentFactory(RoutineCharacteristicsArgumentFactory.class),
})
@DefineFunctionNamespacesTable
@DefineSqlFunctionsTable
@DefineUserDefinedTypesTable
public interface FunctionNamespaceDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS <function_namespaces_table> (\n" +
            "   catalog_name varchar(128) NOT NULL,\n" +
            "   schema_name varchar(128) NOT NULL,\n" +
            "   PRIMARY KEY (catalog_name, schema_name))")
    void createFunctionNamespacesTableIfNotExists();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS <sql_functions_table> (\n" +
            "  id bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  function_id_hash varchar(128) NOT NULL,\n" +
            "  function_id text NOT NULL,\n" +
            "  version bigint(20) unsigned NOT NULL,\n" +
            "  catalog_name varchar(128) NOT NULL,\n" +
            "  schema_name varchar(128) NOT NULL,\n" +
            "  function_name varchar(256) NOT NULL,\n" +
            "  parameters text NOT NULL,\n" +
            "  return_type text NOT NULL,\n" +
            "  routine_characteristics text NOT NULL,\n" +
            "  body mediumtext,\n" +
            "  description text,\n" +
            "  deleted boolean NOT NULL DEFAULT false,\n" +
            "  delete_time TIMESTAMP NULL DEFAULT NULL,\n" +
            "  create_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "  update_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
            "  PRIMARY KEY (id),\n" +
            "  KEY function_id_hash_version (function_id_hash, version),\n" +
            "  KEY qualified_function_name (catalog_name, schema_name, function_name))")
    void createSqlFunctionsTableIfNotExists();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS <user_defined_types_table> (\n" +
            "   id bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  catalog_name varchar(128) NOT NULL,\n" +
            "  schema_name varchar(128) NOT NULL,\n" +
            "  type_name varchar(256) NOT NULL,\n" +
            "  physical_type text NOT NULL,\n" +
            "  PRIMARY KEY (id), \n" +
            "  UNIQUE KEY type_name (catalog_name, schema_name, type_name))")
    void createUserDefinedTypesTableIfNotExists();

    @SqlQuery("SELECT\n" +
            "   count(1) > 0\n" +
            "FROM <function_namespaces_table>\n" +
            "WHERE catalog_name = :catalog_name\n" +
            "  AND schema_name = :schema_name")
    boolean functionNamespaceExists(
            @Bind("catalog_name") String catalogName,
            @Bind("schema_name") String schemaName);

    @SqlQuery("SELECT\n" +
            "    t.catalog_name,\n" +
            "    t.schema_name,\n" +
            "    t.function_name,\n" +
            "    t.parameters,\n" +
            "    t.return_type,\n" +
            "    t.description,\n" +
            "    t.routine_characteristics,\n" +
            "    t.body,\n" +
            "    t.version\n" +
            "FROM <sql_functions_table> t\n" +
            "JOIN (\n" +
            "    SELECT\n" +
            "        function_id_hash,\n" +
            "        function_id,\n" +
            "        MAX(version) version\n" +
            "    FROM <sql_functions_table>\n" +
            "    WHERE catalog_name = :catalog_name\n" +
            "    GROUP BY\n" +
            "        function_id_hash,\n" +
            "        function_id\n" +
            ") v\n" +
            "    ON t.function_id_hash = v.function_id_hash\n" +
            "    AND t.function_id = v.function_id\n " +
            "    AND t.version = v.version\n" +
            "WHERE\n" +
            "    NOT t.deleted")
    List<SqlInvokedFunction> listFunctions(@Bind("catalog_name") String catalogNames);
    @SqlQuery("SELECT\n" +
            "    t.catalog_name,\n" +
            "    t.schema_name,\n" +
            "    t.function_name,\n" +
            "    t.parameters,\n" +
            "    t.return_type,\n" +
            "    t.description,\n" +
            "    t.routine_characteristics,\n" +
            "    t.body,\n" +
            "    t.version\n" +
            "FROM <sql_functions_table> t\n" +
            "JOIN (\n" +
            "    SELECT\n" +
            "        function_id_hash,\n" +
            "        function_id,\n" +
            "        MAX(version) version\n" +
            "    FROM <sql_functions_table>\n" +
            "    WHERE catalog_name = :catalog_name" +
            "      AND concat(catalog_name, '.', schema_name, '.', function_name) like :like_pattern escape :escape\n" +
            "    GROUP BY\n" +
            "        function_id_hash,\n" +
            "        function_id\n" +
            ") v\n" +
            "    ON t.function_id_hash = v.function_id_hash\n" +
            "    AND t.function_id = v.function_id\n " +
            "    AND t.version = v.version\n" +
            "WHERE\n" +
            "    NOT t.deleted")
    List<SqlInvokedFunction> listFunctions(@Bind("catalog_name") String catalogNames, @Bind("like_pattern") String likePattern, @Bind("escape") String escape);

    @SqlQuery("SELECT\n" +
            "    t.catalog_name,\n" +
            "    t.schema_name,\n" +
            "    t.function_name,\n" +
            "    t.parameters,\n" +
            "    t.return_type,\n" +
            "    t.description,\n" +
            "    t.routine_characteristics,\n" +
            "    t.body,\n" +
            "    t.version\n" +
            "FROM <sql_functions_table> t\n" +
            "JOIN (\n" +
            "    SELECT\n" +
            "        function_id_hash,\n" +
            "        function_id,\n" +
            "        MAX(version) version\n" +
            "    FROM <sql_functions_table>\n" +
            "    WHERE catalog_name = :catalog_name\n" +
            "      AND schema_name = :schema_name\n" +
            "      AND function_name = :function_name\n" +
            "    GROUP BY\n" +
            "        function_id_hash,\n" +
            "        function_id\n" +
            ") v\n" +
            "    ON t.function_id_hash = v.function_id_hash\n " +
            "    AND t.function_id = v.function_id\n " +
            "    AND t.version = v.version\n" +
            "WHERE\n" +
            "    NOT t.deleted")
    List<SqlInvokedFunction> getFunctions(
            @Bind("catalog_name") String catalogName,
            @Bind("schema_name") String schemaName,
            @Bind("function_name") String functionName);

    @SqlQuery("SELECT\n" +
            "    catalog_name,\n" +
            "    schema_name,\n" +
            "    function_name,\n" +
            "    parameters,\n" +
            "    return_type,\n" +
            "    description,\n" +
            "    routine_characteristics,\n" +
            "    body,\n" +
            "    version\n" +
            "FROM <sql_functions_table>\n" +
            "WHERE\n" +
            "    function_id_hash = :function_id_hash\n" +
            "    AND function_id = :function_id\n" +
            "    AND version = :version")
    Optional<SqlInvokedFunction> getFunction(
            @Bind("function_id_hash") String functionIdHash,
            @Bind("function_id") SqlFunctionId functionId,
            @Bind("version") long version);

    @SqlQuery("SELECT\n" +
            "    t.catalog_name,\n" +
            "    t.schema_name,\n" +
            "    t.function_name,\n" +
            "    t.parameters,\n" +
            "    t.return_type,\n" +
            "    t.description,\n" +
            "    t.routine_characteristics,\n" +
            "    t.body,\n" +
            "    t.version,\n" +
            "    t.deleted\n" +
            "FROM <sql_functions_table> t\n" +
            "JOIN (\n" +
            "    SELECT\n" +
            "        MAX(version) version\n" +
            "    FROM <sql_functions_table>\n" +
            "    WHERE\n" +
            "        function_id_hash = :function_id_hash\n" +
            "        AND function_id = :function_id\n" +
            ") v\n" +
            "ON\n" +
            "    t.version = v.version\n" +
            "WHERE\n" +
            "    t.function_id_hash = :function_id_hash\n" +
            "    AND t.function_id = :function_id\n" +
            "FOR UPDATE")
    Optional<SqlInvokedFunctionRecord> getLatestRecordForUpdate(
            @Bind("function_id_hash") String functionIdHash,
            @Bind("function_id") SqlFunctionId functionId);

    @SqlQuery("SELECT\n" +
            "    t.catalog_name,\n" +
            "    t.schema_name,\n" +
            "    t.function_name,\n" +
            "    t.parameters,\n" +
            "    t.return_type,\n" +
            "    t.description,\n" +
            "    t.routine_characteristics,\n" +
            "    t.body,\n" +
            "    t.version,\n" +
            "    t.deleted\n" +
            "FROM <sql_functions_table> t\n" +
            "JOIN (\n" +
            "    SELECT\n" +
            "        function_id_hash,\n" +
            "        function_id,\n" +
            "        MAX(version) version\n" +
            "    FROM <sql_functions_table>\n" +
            "    WHERE\n" +
            "        catalog_name = :catalog_name\n" +
            "        AND schema_name = :schema_name\n" +
            "        AND function_name = :function_name\n" +
            "    GROUP BY\n" +
            "        function_id_hash,\n" +
            "        function_id\n" +
            ") v\n" +
            "ON\n" +
            "    t.function_id_hash = v.function_id_hash\n" +
            "    AND t.function_id = v.function_id\n" +
            "    AND t.version = v.version\n" +
            "FOR UPDATE")
    List<SqlInvokedFunctionRecord> getLatestRecordsForUpdate(
            @Bind("catalog_name") String catalogName,
            @Bind("schema_name") String schemaName,
            @Bind("function_name") String functionName);

    @SqlUpdate("INSERT INTO <sql_functions_table> (\n" +
            "        function_id_hash,\n" +
            "        function_id,\n" +
            "        version,\n" +
            "        catalog_name,\n" +
            "        schema_name,\n" +
            "        function_name,\n" +
            "        parameters,\n" +
            "        return_type,\n" +
            "        description,\n" +
            "        routine_characteristics,\n" +
            "        body\n" +
            "    )\n" +
            "VALUES\n" +
            "    (\n" +
            "        :function_id_hash,\n" +
            "        :function_id,\n" +
            "        :version,\n" +
            "        :catalog_name,\n" +
            "        :schema_name,\n" +
            "        :function_name,\n" +
            "        :parameters,\n" +
            "        :return_type,\n" +
            "        :description,\n" +
            "        :routine_characteristics,\n" +
            "        :body\n" +
            "    )")
    void insertFunction(
            @Bind("function_id_hash") String functionIdHash,
            @Bind("function_id") SqlFunctionId functionId,
            @Bind("version") long version,
            @Bind("catalog_name") String catalogName,
            @Bind("schema_name") String schemaName,
            @Bind("function_name") String functionName,
            @Bind("parameters") List<Parameter> parameters,
            @Bind("return_type") TypeSignature returnType,
            @Bind("description") String description,
            @Bind("routine_characteristics") RoutineCharacteristics routineCharacteristics,
            @Bind("body") String body);

    @SqlUpdate("UPDATE\n" +
            "    <sql_functions_table>\n" +
            "SET\n" +
            "    deleted = :deleted\n," +
            "    delete_time = IF(:deleted, NOW(), null)\n" +
            "WHERE\n" +
            "    function_id_hash = :function_id_hash\n" +
            "    AND function_id = :function_id\n" +
            "    AND version = :version")
    int setDeletionStatus(
            @Bind("function_id_hash") String functionIdHash,
            @Bind("function_id") SqlFunctionId functionId,
            @Bind("version") long version,
            @Bind("deleted") boolean deleted);

    @SqlUpdate("UPDATE\n" +
            "    <sql_functions_table>\n" +
            "SET\n" +
            "    deleted = true\n," +
            "    delete_time = NOW()\n" +
            "WHERE\n" +
            "    catalog_name = :catalog_name\n" +
            "    AND schema_name = :schema_name\n" +
            "    AND function_name = :function_name\n" +
            "    AND not deleted")
    int setDeleted(
            @Bind("catalog_name") String catalogName,
            @Bind("schema_name") String schemaName,
            @Bind("function_name") String functionName);

    @SqlQuery("SELECT\n" +
            "   count(1) > 0\n" +
            "FROM <user_defined_types_table>\n" +
            "WHERE catalog_name = :catalog_name\n" +
            "  AND schema_name = :schema_name\n" +
            "  AND type_name = :type_name")
    boolean typeExists(
            @Bind("catalog_name") String catalogName,
            @Bind("schema_name") String schemaName,
            @Bind("type_name") String typeName);

    @SqlQuery("SELECT\n" +
            "    catalog_name,\n" +
            "    schema_name,\n" +
            "    type_name,\n" +
            "    physical_type\n" +
            "  FROM <user_defined_types_table>\n" +
            "WHERE\n" +
            "    catalog_name = :catalog_name\n" +
            "    AND schema_name = :schema_name\n" +
            "    AND type_name = :type_name")
    Optional<UserDefinedType> getUserDefinedType(
            @Bind("catalog_name") String catalogName,
            @Bind("schema_name") String schemaName,
            @Bind("type_name") String typeName);

    @SqlUpdate("INSERT INTO <user_defined_types_table> (\n" +
            "        catalog_name,\n" +
            "        schema_name,\n" +
            "        type_name,\n" +
            "        physical_type\n" +
            "    )\n" +
            "VALUES\n" +
            "    (\n" +
            "        :catalog_name,\n" +
            "        :schema_name,\n" +
            "        :type_name,\n" +
            "        :physical_type\n" +
            "    )")
    void insertUserDefinedType(
            @Bind("catalog_name") String catalogName,
            @Bind("schema_name") String schemaName,
            @Bind("type_name") String typeName,
            @Bind("physical_type") String physicalType);
}
