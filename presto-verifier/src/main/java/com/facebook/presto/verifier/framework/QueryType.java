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

import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.Statement;

import static com.facebook.presto.verifier.framework.QueryType.Category.DATA_PRODUCING;
import static com.facebook.presto.verifier.framework.QueryType.Category.METADATA_READ;
import static java.util.Objects.requireNonNull;

public enum QueryType
{
    CREATE_TABLE_AS_SELECT(DATA_PRODUCING, CreateTableAsSelect.class),
    INSERT(DATA_PRODUCING, Insert.class),
    QUERY(DATA_PRODUCING, Query.class),
    SHOW_CATALOGS(METADATA_READ, ShowCatalogs.class),
    SHOW_COLUMNS(METADATA_READ, ShowColumns.class),
    SHOW_FUNCTIONS(METADATA_READ, ShowFunctions.class),
    SHOW_SCHEMAS(METADATA_READ, ShowSchemas.class),
    SHOW_SESSION(METADATA_READ, ShowSession.class),
    SHOW_TABLES(METADATA_READ, ShowTables.class);

    public enum Category
    {
        DATA_PRODUCING,
        METADATA_READ
    }

    private final Category category;
    private final Class<? extends Statement> statementClass;

    QueryType(Category category, Class<? extends Statement> statementClass)
    {
        this.category = requireNonNull(category, "category is null");
        this.statementClass = requireNonNull(statementClass, "statementClass is null");
    }

    public static QueryType of(Statement statement)
    {
        for (QueryType queryType : values()) {
            if (queryType.statementClass.isAssignableFrom(statement.getClass())) {
                return queryType;
            }
        }
        throw new UnsupportedQueryTypeException(statement.getClass().getSimpleName());
    }

    public Category getCategory()
    {
        return category;
    }
}
