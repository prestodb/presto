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
package com.facebook.presto.example;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.example.ExampleTable.nameGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;

public class ExampleClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Map<String, Map<String, ExampleTable>> schemas;

    @Inject
    public ExampleClient(ExampleConfig config, JsonCodec<Map<String, List<ExampleTable>>> catalogCodec)
            throws IOException
    {
        checkNotNull(config, "config is null");
        checkNotNull(catalogCodec, "catalogCodec is null");

        final URI metadataUri = config.getMetadata();

        String json = Resources.toString(metadataUri.toURL(), Charsets.UTF_8);
        Map<String, List<ExampleTable>> catalog = catalogCodec.fromJson(json);

        this.schemas = ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    public Set<String> getSchemaNames()
    {
        return schemas.keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        checkNotNull(schema, "schema is null");
        Map<String, ExampleTable> tables = schemas.get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public ExampleTable getTable(String schema, String tableName)
    {
        checkNotNull(schema, "schema is null");
        checkNotNull(tableName, "tableName is null");
        Map<String, ExampleTable> tables = schemas.get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Function<List<ExampleTable>, Map<String, ExampleTable>> resolveAndIndexTables(final URI metadataUri)
    {
        return new Function<List<ExampleTable>, Map<String, ExampleTable>>()
        {
            @Override
            public Map<String, ExampleTable> apply(List<ExampleTable> tables)
            {
                Iterable<ExampleTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
                return ImmutableMap.copyOf(uniqueIndex(resolvedTables, nameGetter()));
            }
        };
    }

    private static Function<ExampleTable, ExampleTable> tableUriResolver(final URI baseUri)
    {
        return new Function<ExampleTable, ExampleTable>()
        {
            @Override
            public ExampleTable apply(ExampleTable table)
            {
                List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), uriResolver(baseUri)));
                return new ExampleTable(table.getName(), table.getColumns(), sources);
            }
        };
    }

    private static Function<URI, URI> uriResolver(final URI baseUri)
    {
        return new Function<URI, URI>()
        {
            @Override
            public URI apply(URI source)
            {
                return baseUri.resolve(source);
            }
        };
    }
}
