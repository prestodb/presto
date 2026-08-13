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
package com.facebook.presto.iceberg.rest;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A REST catalog that accepts a custom {@link FileIO} factory, which Iceberg's
 * {@link org.apache.iceberg.rest.RESTCatalog} cannot: every one of its constructors hardcodes
 * {@code ioBuilder = null} and its {@code RESTSessionCatalog} field is private. Without an
 * {@code ioBuilder}, {@code RESTSessionCatalog.tableFileIO} hands back one catalog-level FileIO
 * for every table whenever the server returns no per-table config or credentials, making it
 * impossible to bind a per-identity FileIO without mutating shared state.
 * <p>
 * Delegation mirrors {@code RESTCatalog}. The {@code createTable},
 * {@code newCreateTableTransaction} and {@code newReplaceTableTransaction} overloads are left to
 * the {@link Catalog} defaults, which route through {@link #buildTable} -- what Iceberg's own
 * {@code BaseSessionCatalog.AsCatalog} relies on.
 */
public class PrestoRestCatalog
        implements Catalog, ViewCatalog, SupportsNamespaces, Configurable<Object>, Closeable
{
    private final RESTSessionCatalog sessionCatalog;
    private final Catalog delegate;
    private final SupportsNamespaces namespaceDelegate;
    private final ViewCatalog viewDelegate;

    public PrestoRestCatalog(
            SessionContext context,
            Function<Map<String, String>, RESTClient> clientBuilder,
            BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder)
    {
        requireNonNull(context, "context is null");
        requireNonNull(clientBuilder, "clientBuilder is null");
        requireNonNull(ioBuilder, "ioBuilder is null");
        this.sessionCatalog = new RESTSessionCatalog(clientBuilder, ioBuilder);
        this.delegate = sessionCatalog.asCatalog(context);
        this.namespaceDelegate = (SupportsNamespaces) delegate;
        this.viewDelegate = sessionCatalog.asViewCatalog(context);
    }

    @Override
    public void initialize(String name, Map<String, String> properties)
    {
        sessionCatalog.initialize(name, requireNonNull(properties, "properties is null"));
    }

    @Override
    public String name()
    {
        return sessionCatalog.name();
    }

    public Map<String, String> properties()
    {
        return sessionCatalog.properties();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace)
    {
        return delegate.listTables(namespace);
    }

    @Override
    public boolean tableExists(TableIdentifier identifier)
    {
        return delegate.tableExists(identifier);
    }

    @Override
    public Table loadTable(TableIdentifier identifier)
    {
        return delegate.loadTable(identifier);
    }

    @Override
    public void invalidateTable(TableIdentifier identifier)
    {
        delegate.invalidateTable(identifier);
    }

    @Override
    public TableBuilder buildTable(TableIdentifier identifier, Schema schema)
    {
        return delegate.buildTable(identifier, schema);
    }

    @Override
    public Table registerTable(TableIdentifier identifier, String metadataFileLocation)
    {
        return delegate.registerTable(identifier, metadataFileLocation);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier)
    {
        return delegate.dropTable(identifier);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge)
    {
        return delegate.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to)
    {
        delegate.renameTable(from, to);
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata)
    {
        namespaceDelegate.createNamespace(namespace, metadata);
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace)
            throws NoSuchNamespaceException
    {
        return namespaceDelegate.listNamespaces(namespace);
    }

    @Override
    public boolean namespaceExists(Namespace namespace)
    {
        return namespaceDelegate.namespaceExists(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace)
            throws NoSuchNamespaceException
    {
        return namespaceDelegate.loadNamespaceMetadata(namespace);
    }

    @Override
    public boolean dropNamespace(Namespace namespace)
            throws NamespaceNotEmptyException
    {
        return namespaceDelegate.dropNamespace(namespace);
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties)
            throws NoSuchNamespaceException
    {
        return namespaceDelegate.setProperties(namespace, properties);
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties)
            throws NoSuchNamespaceException
    {
        return namespaceDelegate.removeProperties(namespace, properties);
    }

    @Override
    public List<TableIdentifier> listViews(Namespace namespace)
    {
        return viewDelegate.listViews(namespace);
    }

    @Override
    public View loadView(TableIdentifier identifier)
    {
        return viewDelegate.loadView(identifier);
    }

    @Override
    public ViewBuilder buildView(TableIdentifier identifier)
    {
        return viewDelegate.buildView(identifier);
    }

    @Override
    public boolean viewExists(TableIdentifier identifier)
    {
        return viewDelegate.viewExists(identifier);
    }

    @Override
    public boolean dropView(TableIdentifier identifier)
    {
        return viewDelegate.dropView(identifier);
    }

    @Override
    public void renameView(TableIdentifier from, TableIdentifier to)
    {
        viewDelegate.renameView(from, to);
    }

    @Override
    public void invalidateView(TableIdentifier identifier)
    {
        viewDelegate.invalidateView(identifier);
    }

    @Override
    public void setConf(Object conf)
    {
        sessionCatalog.setConf(conf);
    }

    @Override
    public void close()
            throws IOException
    {
        sessionCatalog.close();
    }
}
