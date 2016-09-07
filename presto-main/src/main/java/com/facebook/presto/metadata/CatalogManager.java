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
package com.facebook.presto.metadata;

import com.facebook.presto.connector.ConnectorId;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class CatalogManager
{
    private final ConcurrentMap<String, Catalog> catalogs = new ConcurrentHashMap<>();

    public synchronized void registerCatalog(Catalog catalog)
    {
        requireNonNull(catalog, "catalog is null");

        checkState(catalogs.put(catalog.getCatalogName(), catalog) == null, "Catalog '%s' is already registered", catalog.getCatalogName());
    }

    public Optional<ConnectorId> removeCatalog(String catalogName)
    {
        return Optional.ofNullable(catalogs.remove(catalogName))
                .map(Catalog::getConnectorId);
    }

    public List<Catalog> getCatalogs()
    {
        return ImmutableList.copyOf(catalogs.values());
    }

    public Optional<Catalog> getCatalog(String catalogName)
    {
        return Optional.ofNullable(catalogs.get(catalogName));
    }
}
