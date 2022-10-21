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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.resourceGroups.QueryType;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class PreparedQuery
{
    private final Optional<String> formattedQuery;
    private final Optional<String> prepareSql;

    public PreparedQuery(Optional<String> formattedQuery, Optional<String> prepareSql)
    {
        this.formattedQuery = requireNonNull(formattedQuery, "formattedQuery is null");
        this.prepareSql = requireNonNull(prepareSql, "prepareSql is null");
    }

    public Optional<String> getFormattedQuery()
    {
        return formattedQuery;
    }

    public Optional<String> getPrepareSql()
    {
        return prepareSql;
    }

    public Optional<QueryType> getQueryType()
    {
        throw new UnsupportedOperationException("getQueryType method is not supported!");
    }

    public boolean isTransactionControlStatement()
    {
        throw new UnsupportedOperationException("isTransactionControlStatement method is not supported!");
    }

    public Class<?> getStatementClass()
    {
        throw new UnsupportedOperationException("getStatementClass method is not supported!");
    }
}
