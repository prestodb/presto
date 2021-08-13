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
package com.facebook.presto.tests;

import com.facebook.presto.functionNamespace.mysql.FunctionNamespaceDao;
import com.facebook.presto.spi.function.SqlFunctionId;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface H2FunctionNamespaceDao
        extends FunctionNamespaceDao
{
    @SqlUpdate("UPDATE\n" +
            "    <sql_functions_table>\n" +
            "SET\n" +
            "    deleted = :deleted\n," +
            "    delete_time = CASEWHEN(:deleted, NOW(), null)\n" +
            "WHERE\n" +
            "    function_id_hash = :function_id_hash\n" +
            "    AND function_id = :function_id\n" +
            "    AND version = :version")
    @Override
    int setDeletionStatus(
            @Bind("function_id_hash") String functionIdHash,
            @Bind("function_id") SqlFunctionId functionId,
            @Bind("version") long version,
            @Bind("deleted") boolean deleted);
}
