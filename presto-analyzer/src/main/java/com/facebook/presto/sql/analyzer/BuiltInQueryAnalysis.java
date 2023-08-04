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

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.AccessControlReferences;
import com.facebook.presto.spi.analyzer.QueryAnalysis;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.sql.tree.Explain;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BuiltInQueryAnalysis
        implements QueryAnalysis
{
    private final Analysis analysis;

    public BuiltInQueryAnalysis(Analysis analysis)
    {
        this.analysis = analysis;
    }

    public Analysis getAnalysis()
    {
        return analysis;
    }

    @Override
    public String getUpdateType()
    {
        return analysis.getUpdateType();
    }

    @Override
    public Optional<String> getExpandedQuery()
    {
        return analysis.getExpandedQuery();
    }

    @Override
    public Map<FunctionKind, Set<String>> getInvokedFunctions()
    {
        return analysis.getInvokedFunctions();
    }

    @Override
    public AccessControlReferences getAccessControlReferences()
    {
        return analysis.getAccessControlReferences();
    }

    @Override
    public boolean isExplainAnalyzeQuery()
    {
        return analysis.getStatement() instanceof Explain && ((Explain) analysis.getStatement()).isAnalyze();
    }

    @Override
    public Set<ConnectorId> extractConnectors()
    {
        ImmutableSet.Builder<ConnectorId> connectors = ImmutableSet.builder();

        for (TableHandle tableHandle : analysis.getTables()) {
            connectors.add(tableHandle.getConnectorId());
        }

        if (analysis.getInsert().isPresent()) {
            TableHandle target = analysis.getInsert().get().getTarget();
            connectors.add(target.getConnectorId());
        }

        return connectors.build();
    }
}
