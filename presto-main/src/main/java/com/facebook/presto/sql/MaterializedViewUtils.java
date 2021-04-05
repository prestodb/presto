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

package com.facebook.presto.sql;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.MaterializedViewPlanValidator;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.ConnectorMaterializedViewDefinition.TableColumn;
import static com.facebook.presto.sql.analyzer.MaterializedViewPlanValidator.MaterializedViewPlanValidatorContext;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class MaterializedViewUtils
{
    private MaterializedViewUtils() {}

    public static void validateMaterializedViewQueryPlan(Statement query)
    {
        MaterializedViewPlanValidator validator = new MaterializedViewPlanValidator(query);
        validator.process(query, new MaterializedViewPlanValidatorContext());
    }

    public static Map<String, Map<SchemaTableName, String>> computeMaterializedViewToBaseTableColumnMappings(Query viewQuery, Analysis analysis)
    {
        ImmutableMap.Builder<String, Map<SchemaTableName, String>> fullColumnMapping = ImmutableMap.builder();

        Map<String, Map<SchemaTableName, String>> originalColumnMapping = getOriginalColumnMappingFromAnalysis(analysis, viewQuery);

        MaterializedViewPlanValidator planAnalyzer = new MaterializedViewPlanValidator(viewQuery, true, Optional.of(analysis));
        planAnalyzer.process(viewQuery, new MaterializedViewPlanValidatorContext());
        List<List<TableColumn>> linkedBaseColumns = planAnalyzer.getLinkedBaseColumns();

        for (Map.Entry<String, Map<SchemaTableName, String>> entry : originalColumnMapping.entrySet()) {
            String viewColumn = entry.getKey();
            Map<SchemaTableName, String> originalBaseColumns = entry.getValue();

            Map<SchemaTableName, String> fullBaseColumns = new HashMap<SchemaTableName, String>() {{
                    putAll(originalBaseColumns); }};

            originalBaseColumns.forEach((originalBaseTable, originalBaseColumn) -> {
                TableColumn originalTableColumn = new TableColumn(originalBaseTable, originalBaseColumn);
                linkedBaseColumns.forEach(linkedTableColumnPair -> {
                    if (originalTableColumn.equals(linkedTableColumnPair.get(0))) {
                        fullBaseColumns.put(linkedTableColumnPair.get(1).getTableName(), linkedTableColumnPair.get(1).getColumnName());
                    }
                    else if (originalTableColumn.equals(linkedTableColumnPair.get(1))) {
                        fullBaseColumns.put(linkedTableColumnPair.get(0).getTableName(), linkedTableColumnPair.get(0).getColumnName());
                    }
                });
            });

            fullColumnMapping.put(viewColumn, ImmutableMap.copyOf(fullBaseColumns));
        }

        return fullColumnMapping.build();
    }

    private static Map<String, Map<SchemaTableName, String>> getOriginalColumnMappingFromAnalysis(Analysis analysis, Node node)
    {
        return analysis.getOutputDescriptor(node).getVisibleFields().stream()
                .filter(field -> field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent())
                .collect(toImmutableMap(
                        field -> field.getName().get(),
                        field -> ImmutableMap.of(toSchemaTableName(field.getOriginTable().get()), field.getOriginColumnName().get())));
    }
}
