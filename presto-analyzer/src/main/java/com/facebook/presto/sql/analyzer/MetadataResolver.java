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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.ViewDefinition;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface MetadataResolver
{
    boolean catalogExists(String catalogName);

    boolean schemaExists(CatalogSchemaName schema);

    boolean tableExists(QualifiedObjectName tableName);

    List<ColumnMetadata> getColumns(QualifiedObjectName tableName);

    Optional<TableHandle> getTableHandle(QualifiedObjectName tableName);

    Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle);

    TableMetadata getTableMetadata(TableHandle tableHandle);

    Optional<ConnectorId> getCatalogHandle(String catalogName);

    Optional<ViewDefinition> getView(QualifiedObjectName viewName);

    Optional<ConnectorMaterializedViewDefinition> getMaterializedView(QualifiedObjectName viewName);

    MaterializedViewStatus getMaterializedViewStatus(QualifiedObjectName materializedViewName, TupleDomain<String> baseQueryDomain);
}
