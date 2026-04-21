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
package com.facebook.presto.sql.analyzer.procedure;

import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.procedure.ProcedureAnalysisContext;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.google.errorprone.annotations.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class TableDataRewriteAnalysisContext
        implements ProcedureAnalysisContext
{
    private final TableHandle callTarget;
    private final QuerySpecification targetQuery;

    public TableDataRewriteAnalysisContext(
            TableHandle callTarget,
            QuerySpecification targetQuery)
    {
        this.callTarget = requireNonNull(callTarget, "callTarget is null");
        this.targetQuery = requireNonNull(targetQuery, "targetQuery is null");
    }

    public TableHandle getCallTarget()
    {
        return callTarget;
    }

    public QuerySpecification getTargetQuery()
    {
        return targetQuery;
    }
}
