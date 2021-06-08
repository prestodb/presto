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

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MaterializedViewValidationForQueryRewrite<R, C>
        extends DefaultTraversalVisitor<R, C>
{
    private final List<QualifiedName> tableNames = new ArrayList<>();
    private final Map<String, List<String>> baseTableToMaterializedViewMap;

    public MaterializedViewValidationForQueryRewrite(Map<String, List<String>> baseTableToMaterializedViewMap)
    {
        this.baseTableToMaterializedViewMap = baseTableToMaterializedViewMap;
    }

    @Override
    protected R visitTable(Table node, C context)
    {
        tableNames.add(node.getName());
        return null;
    }

    protected boolean canOptimize()
    {
        for (QualifiedName baseTable : tableNames) {
            if (baseTableToMaterializedViewMap.containsKey(baseTable.getSuffix())) {
                return true;
            }
        }
        return false;
    }

    protected Set<String> generateMaterializedViewCandidate()
    {
        Set<String> materializedViewCandidate = new HashSet();
        for (QualifiedName baseTable : tableNames) {
            if (baseTableToMaterializedViewMap.containsKey(baseTable.getSuffix())) {
                List<String> materializedViewList = baseTableToMaterializedViewMap.get(baseTable.getSuffix());
                materializedViewCandidate.addAll(materializedViewList);
            }
        }
        return materializedViewCandidate;
    }
}
