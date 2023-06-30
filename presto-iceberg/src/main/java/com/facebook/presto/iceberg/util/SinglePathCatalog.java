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
package com.facebook.presto.iceberg.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This class is an implementation of an Iceberg catalog that can load a table from a single
 * path in a filesystem. It must directly point to the top-level directory of a single table
 * which contains all the standard files and metadata for a table.
 * <p>
 * The path location must be accessible through the Hadoop {@link FileSystem} client.
 * <p>
 * There is no namespacing associated with this catalog. It will always return a single table
 * no matter the namespace that is provided. You cannot drop or rename the table either.
 */
public class SinglePathCatalog
        extends HadoopCatalog implements AutoCloseable
{
    private final Path path;
    private final Configuration conf;

    public SinglePathCatalog(Path path, Configuration conf)
    {
        this.path = requireNonNull(path);
        this.conf = requireNonNull(conf);
    }

    @Override
    public void initialize(String name, Map<String, String> properties)
    {
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, path.toString());
        this.setConf(this.conf);
        super.initialize(name, properties);
    }

    @Override
    public void close() throws IOException
    {
        super.close();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace)
    {
        return Collections.singletonList(TableIdentifier.of(namespace, path.getName()));
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge)
    {
        throw unsupportedOp();
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to)
    {
        throw unsupportedOp();
    }

    @Override
    public void createNamespace(Namespace namespace)
    {
        throw unsupportedOp();
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> meta)
    {
        throw unsupportedOp();
    }

    @Override
    public boolean dropNamespace(Namespace namespace)
    {
        throw unsupportedOp();
    }

    @Override
    public Table registerTable(TableIdentifier identifier, String metadataFileLocation)
    {
        throw unsupportedOp();
    }

    private RuntimeException unsupportedOp()
    {
        return new UnsupportedOperationException("Can't " + getCallerMethodName(2) + " in " + SinglePathCatalog.class.getName());
    }

    private String getCallerMethodName(int idx)
    {
        return new Throwable().getStackTrace()[idx].getMethodName();
    }
}
