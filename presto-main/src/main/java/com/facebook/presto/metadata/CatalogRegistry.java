/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.metadata;

import com.google.inject.Inject;

import javax.annotation.concurrent.GuardedBy;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class CatalogRegistry
{
    private final Map<String, DataSourceType> exactTypes = new LinkedHashMap<>();
    @GuardedBy("exactTypes")
    private final Map<String, DataSourceType> prefixTypes = new LinkedHashMap<>();

    @Inject
    public CatalogRegistry()
    {
    }

    public DataSourceType getCatalogType(String catalogName)
    {
        synchronized (exactTypes) {
            DataSourceType type = exactTypes.get(catalogName);

            if (type == null) {
                for (Map.Entry<String, DataSourceType> entry : prefixTypes.entrySet()) {
                    String prefix = entry.getKey();

                    if (catalogName.startsWith(prefix)) {
                        type = entry.getValue();
                    }
                }
            }

            return type;
        }
    }

    public CatalogRegistry addExactType(String catalogName, DataSourceType type)
    {
        synchronized (exactTypes) {
            DataSourceType existing = exactTypes.put(catalogName, type);

            checkState(existing == null, "Catalog name %s added twice with types %s and %s", catalogName, existing, type);

            return this;
        }
    }

    public CatalogRegistry addPrefixType(String catalogNamePrefix, DataSourceType type)
    {
        synchronized (exactTypes) {
            DataSourceType existing = prefixTypes.put(catalogNamePrefix, type);

            checkState(existing == null, "Catalog name prefix %s added twice with types %s and %s", catalogNamePrefix, existing, type);

            for (String exactName : exactTypes.keySet()) {
                checkState(!catalogNamePrefix.equals(exactName), "Catalog name prefix %s conflicts with exact name %s", catalogNamePrefix, exactName);
            }

            return this;
        }
    }
}
