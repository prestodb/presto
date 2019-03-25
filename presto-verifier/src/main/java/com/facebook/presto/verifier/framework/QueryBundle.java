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

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class QueryBundle
{
    private final Optional<QualifiedName> tableName;
    private final List<Statement> setupQueries;
    private final Statement query;
    private final List<Statement> teardownQueries;

    public QueryBundle(
            Optional<QualifiedName> tableName,
            List<Statement> setupQueries,
            Statement query,
            List<Statement> teardownQueries)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.setupQueries = ImmutableList.copyOf(setupQueries);
        this.query = requireNonNull(query, "mainQueries is null");
        this.teardownQueries = ImmutableList.copyOf(teardownQueries);
    }

    public QualifiedName getTableName()
    {
        checkState(tableName.isPresent(), "tableName is missing");
        return tableName.get();
    }

    public List<Statement> getSetupQueries()
    {
        return setupQueries;
    }

    public Statement getQuery()
    {
        return query;
    }

    public List<Statement> getTeardownQueries()
    {
        return teardownQueries;
    }
}
