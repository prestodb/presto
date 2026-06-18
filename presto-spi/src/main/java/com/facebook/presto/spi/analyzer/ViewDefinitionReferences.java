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
package com.facebook.presto.spi.analyzer;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.MaterializedViewDefinition;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class ViewDefinitionReferences
{
    private final Map<QualifiedObjectName, ViewDefinition> viewDefinitions;
    private final Map<QualifiedObjectName, MaterializedViewDefinition> materializedViewDefinitions;

    public ViewDefinitionReferences()
    {
        viewDefinitions = new LinkedHashMap<>();
        materializedViewDefinitions = new LinkedHashMap<>();
    }

    public void addViewDefinitionReference(QualifiedObjectName viewDefinitionName, ViewDefinition viewDefinition)
    {
        viewDefinitions.put(viewDefinitionName, viewDefinition);
    }

    public void addMaterializedViewDefinitionReference(QualifiedObjectName viewDefinitionName, MaterializedViewDefinition materializedViewDefinition)
    {
        materializedViewDefinitions.put(viewDefinitionName, materializedViewDefinition);
    }

    public Map<QualifiedObjectName, ViewDefinition> getViewDefinitions()
    {
        return unmodifiableMap(new LinkedHashMap<>(viewDefinitions));
    }

    public Map<QualifiedObjectName, MaterializedViewDefinition> getMaterializedViewDefinitions()
    {
        return unmodifiableMap(new LinkedHashMap<>(materializedViewDefinitions));
    }
}
