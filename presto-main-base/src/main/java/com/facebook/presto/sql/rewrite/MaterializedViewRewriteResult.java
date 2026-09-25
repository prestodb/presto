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
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.sql.tree.Statement;

import static java.util.Objects.requireNonNull;

public final class MaterializedViewRewriteResult
{
    private final Statement statement;
    private final boolean materializedViewOptimizationApplied;

    public static MaterializedViewRewriteResult materializedViewRewriteResult(Statement statement, boolean materializedViewOptimizationApplied)
    {
        return new MaterializedViewRewriteResult(statement, materializedViewOptimizationApplied);
    }

    private MaterializedViewRewriteResult(Statement statement, boolean materializedViewOptimizationApplied)
    {
        this.statement = requireNonNull(statement, "statement is null");
        this.materializedViewOptimizationApplied = materializedViewOptimizationApplied;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public boolean isMaterializedViewOptimizationApplied()
    {
        return materializedViewOptimizationApplied;
    }
}
