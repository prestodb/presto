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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;

import java.util.Optional;

public enum QueryType
{
    CREATE_TABLE_AS_SELECT(CreateTableAsSelect.class),
    INSERT(Insert.class),
    QUERY(Query.class),
    CREATE_VIEW(CreateView.class),
    CREATE_TABLE(CreateTable.class),
    UNSUPPORTED();

    private final Optional<Class<? extends Statement>> statementClass;

    QueryType(Class<? extends Statement> statementClass)
    {
        this.statementClass = Optional.of(statementClass);
    }

    QueryType()
    {
        this.statementClass = Optional.empty();
    }

    public static QueryType of(Statement statement)
    {
        for (QueryType queryType : values()) {
            if (queryType.statementClass.isPresent() && queryType.statementClass.get().isAssignableFrom(statement.getClass())) {
                return queryType;
            }
        }
        return UNSUPPORTED;
    }
}
