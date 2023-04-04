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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.analyzer.ViewDefinition;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;

public class MetadataHandle
{
    private boolean preProcessMetadataCalls;
    private final Map<QualifiedObjectName, Future<Optional<ViewDefinition>>> viewDefinitions = new ConcurrentHashMap<>();
    private final Map<QualifiedObjectName, Future<Optional<MaterializedViewDefinition>>> materializedViewDefinitions = new ConcurrentHashMap<>();
    private final Map<QualifiedObjectName, Future<TableColumnMetadata>> tableColumnsMetadata = new ConcurrentHashMap<>();

    public boolean isPreProcessMetadataCalls()
    {
        return preProcessMetadataCalls;
    }

    public void setPreProcessMetadataCalls(boolean preProcessMetadataCalls)
    {
        this.preProcessMetadataCalls = preProcessMetadataCalls;
    }

    public void addViewDefinition(QualifiedObjectName viewName, Future<Optional<ViewDefinition>> viewDefinition)
    {
        checkState(!viewDefinitions.containsKey(viewName), "View " + viewName + " already exists");
        this.viewDefinitions.put(viewName, viewDefinition);
    }

    public void addMaterializedViewDefinition(QualifiedObjectName viewName, Future<Optional<MaterializedViewDefinition>> viewDefinition)
    {
        checkState(!materializedViewDefinitions.containsKey(viewName), "View " + viewName + " already exists");
        this.materializedViewDefinitions.put(viewName, viewDefinition);
    }

    public void addTableColumnMetadata(QualifiedObjectName tableName, Future<TableColumnMetadata> tableColumnMetadata)
    {
        checkState(!tableColumnsMetadata.containsKey(tableName), "Table " + tableName + " already present");
        this.tableColumnsMetadata.put(tableName, tableColumnMetadata);
    }

    public Optional<ViewDefinition> getViewDefinition(QualifiedObjectName viewName)
    {
        checkState(viewDefinitions.containsKey(viewName), "View " + viewName + " not found, the available view names are " + viewDefinitions.keySet());
        try {
            return viewDefinitions.get(viewName).get();
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        catch (ExecutionException ex) {
            handleException(ex);
            throw new RuntimeException(ex.getCause());
        }
    }

    public Optional<MaterializedViewDefinition> getMaterializedViewDefinition(QualifiedObjectName viewName)
    {
        checkState(materializedViewDefinitions.containsKey(viewName), "View " + viewName + " not found");
        try {
            return materializedViewDefinitions.get(viewName).get();
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        catch (ExecutionException ex) {
            handleException(ex);
            throw new RuntimeException(ex.getCause());
        }
    }

    public TableColumnMetadata getTableColumnsMetadata(QualifiedObjectName tableName)
    {
        checkState(tableColumnsMetadata.containsKey(tableName), "Table " + tableName + " not found");
        try {
            return tableColumnsMetadata.get(tableName).get();
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        catch (ExecutionException ex) {
            handleException(ex);
            throw new RuntimeException(ex.getCause());
        }
    }

    private void handleException(ExecutionException ex)
            throws RuntimeException
    {
        Throwable cause = ex.getCause();
        if (cause instanceof SemanticException) {
            throw (SemanticException) cause;
        }
    }
}
