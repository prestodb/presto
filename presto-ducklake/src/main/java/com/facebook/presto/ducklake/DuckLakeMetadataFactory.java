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
package com.facebook.presto.ducklake;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.ducklake.catalog.DuckLakeCatalog;
import com.facebook.presto.ducklake.statistics.TableStatisticsMaker;
import jakarta.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Builds one fresh {@link DuckLakeMetadata} instance per {@code beginTransaction} call, the way
 * {@code IcebergMetadataFactory} builds one {@code IcebergTransactionMetadata} per transaction.
 * Each instance keeps its own snapshot-scoped cache of catalog lookups, so a query sees one
 * consistent snapshot even as later transactions resolve a newer one.
 */
public class DuckLakeMetadataFactory
{
    private final DuckLakeCatalog catalog;
    private final TypeManager typeManager;
    private final TableStatisticsMaker tableStatisticsMaker;

    @Inject
    public DuckLakeMetadataFactory(DuckLakeCatalog catalog, TypeManager typeManager, TableStatisticsMaker tableStatisticsMaker)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableStatisticsMaker = requireNonNull(tableStatisticsMaker, "tableStatisticsMaker is null");
    }

    public DuckLakeMetadata create()
    {
        return new DuckLakeMetadata(catalog, typeManager, tableStatisticsMaker);
    }
}
