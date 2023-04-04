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
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.ViewDefinition;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;

public class MetadataHandle
{
    Map<QualifiedObjectName, Future<Optional<ViewDefinition>>> viewDefinitions = new ConcurrentHashMap<>();
    Map<QualifiedObjectName, Future<Optional<MaterializedViewDefinition>>> materializedViewDefinitions = new ConcurrentHashMap<>();
    Map<QualifiedObjectName, Future<Optional<TableHandle>>> tableHandles = new ConcurrentHashMap<>();

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

    public void addTableHandle(QualifiedObjectName tableName, Future<Optional<TableHandle>> tableHandle)
    {
        checkState(!tableHandles.containsKey(tableName), "Table " + tableName + " already present");
        this.tableHandles.put(tableName, tableHandle);
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
            throw new RuntimeException(ex);
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
            throw new RuntimeException(ex);
        }
    }

    public Optional<TableHandle> getTableHandle(QualifiedObjectName tableName)
    {
        checkState(tableHandles.containsKey(tableName), "Table " + tableName + " not found");
        try {
            return tableHandles.get(tableName).get();
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        }
    }
}
