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
package com.facebook.presto.verifier.source;

import com.facebook.presto.verifier.framework.SourceQuery;
import org.jdbi.v3.sqlobject.config.RegisterColumnMapper;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.Define;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;

@RegisterColumnMapper(StringToStringMapColumnMapper.class)
public interface VerifierDao
{
    @SqlUpdate("CREATE TABLE verifier_queries (\n" +
            "  id int(11) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,\n" +
            "  suite varchar(256) NOT NULL,\n" +
            "  name varchar(256) DEFAULT NULL,\n" +
            "  control_catalog varchar(256) NOT NULL,\n" +
            "  control_schema varchar(256) NOT NULL,\n" +
            "  control_query text NOT NULL,\n" +
            "  control_username varchar(256) DEFAULT NULL,\n" +
            "  control_password varchar(256) DEFAULT NULL,\n" +
            "  control_session_properties text DEFAULT NULL,\n" +
            "  test_catalog varchar(256) NOT NULL,\n" +
            "  test_schema varchar(256) NOT NULL,\n" +
            "  test_query text NOT NULL,\n" +
            "  test_username varchar(256) DEFAULT NULL,\n" +
            "  test_password varchar(256) DEFAULT NULL,\n" +
            "  test_session_properties text DEFAULT NULL)")
    void createVerifierQueriesTable(@Define("table_name") String tableName);

    @SqlQuery("SELECT\n" +
            "  suite,\n" +
            "  name,\n" +
            "  control_query,\n" +
            "  control_catalog,\n" +
            "  control_schema,\n" +
            "  control_username,\n" +
            "  control_password,\n" +
            "  control_session_properties,\n" +
            "  test_query,\n" +
            "  test_catalog,\n" +
            "  test_schema,\n" +
            "  test_username,\n" +
            "  test_password,\n" +
            "  test_session_properties\n" +
            "FROM\n" +
            "  <table_name>\n" +
            "WHERE\n" +
            "  suite  = :suite\n" +
            "ORDER BY\n" +
            "  id\n" +
            "LIMIT\n" +
            "  :limit")
    @RegisterConstructorMapper(SourceQuery.class)
    List<SourceQuery> getSourceQueries(
            @Define("table_name") String tableName,
            @Bind("suite") String suite,
            @Bind("limit") int limit);
}
