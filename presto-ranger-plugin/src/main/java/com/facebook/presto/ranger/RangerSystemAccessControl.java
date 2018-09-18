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
package com.facebook.presto.ranger;

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import io.airlift.log.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

import java.security.Principal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectColumns;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetUser;
import static com.facebook.presto.spi.security.AccessDeniedException.denyShowTablesMetadata;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Comparator.comparing;

public class RangerSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger log = Logger.get(RangerSystemAccessControl.class);
    private final Set<String> powerPrincipals;

    private final Set<String> powerUsers;
    private final PrestoAuthorizer authorizer;
    private final Set<String> writeableCatalogs;

    private static final char COLUMN_SEP = ',';

    public RangerSystemAccessControl(PrestoAuthorizer prestoAuthorizer, Map<String, String> config)
    {
        this.authorizer = prestoAuthorizer;

        String[] writeableCatalogs = config.getOrDefault("writeable-catalogs", "").split(",");
        this.writeableCatalogs = Arrays.stream(writeableCatalogs).filter(s -> !s.isEmpty()).collect(toImmutableSet());

        log.info("Writeable catalogs: " + this.writeableCatalogs);
        String[] powerPrincipals = config.getOrDefault("power-principals", "")
                .split(",");
        this.powerPrincipals = Arrays.stream(powerPrincipals)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .collect(toImmutableSet());

        String[] powerUsers = config.getOrDefault("power-users", "")
                .split(",");
        this.powerUsers = Arrays.stream(powerUsers)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        if (!principal.isPresent()) {
            return;
        }
        if (powerPrincipals.contains(principal.get().getName().toLowerCase(Locale.ENGLISH))) {
            return;
        }
        String principalName = principal.get().getName()
                .replaceAll("@.*", "")
                .replaceAll("/.*", "");
        if (!principalName.equalsIgnoreCase(userName)) {
            denySetUser(principal, userName);
        }
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        if (!powerUsers.contains(identity.getUser().toLowerCase(Locale.ENGLISH))) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        if (!powerUsers.contains(identity.getUser().toLowerCase(Locale.ENGLISH))) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
        if (!authorizer.canSeeResource(createResource(schema), identity)) {
            denyShowTablesMetadata(schema.getSchemaName());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        List<RangerPrestoResource> rangerResources = tableNames
                .stream()
                .map(t -> new RangerPrestoResource(catalogName, t.getSchemaName(), Optional.of(t.getTableName())))
                .collect(toImmutableList());

        Stream<SchemaTableName> outTables = authorizer
                .filterResources(rangerResources, identity)
                .stream()
                .map(RangerPrestoResource::getSchemaTable);

        return makeSortedSet(outTables, comparing(t -> t.toString().toLowerCase(Locale.ENGLISH)));
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        List<RangerPrestoResource> rangerResources = schemaNames
                .stream()
                .map(schemaName -> new RangerPrestoResource(catalogName, schemaName, Optional.empty()))
                .collect(toImmutableList());

        Stream<String> outSchemas = authorizer
                .filterResources(rangerResources, identity)
                .stream()
                .map(RangerPrestoResource::getDatabase);

        return makeSortedSet(outSchemas, comparing(String::toLowerCase));
    }

    private <T> SortedSet<T> makeSortedSet(Stream<T> it, Comparator<T> comparator)
    {
        SortedSet<T> set = new TreeSet<>(comparator);
        it.forEach(set::add);
        return set;
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!authorizer.canCreateResource(createResource(schema), identity) ||
                !writeableCatalogs.contains(schema.getCatalogName())) {
            denyCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!authorizer.canCreateResource(createResource(schema), identity) ||
                !writeableCatalogs.contains(schema.getCatalogName())) {
            denyDropSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        if (!authorizer.canCreateResource(createResource(schema), identity) ||
                !authorizer.canDropResource(createResource(schema.getCatalogName(), newSchemaName), identity) ||
                !writeableCatalogs.contains(schema.getCatalogName())) {
            denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canCreateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyCreateTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canDropResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyDropTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!authorizer.canCreateResource(createResource(newTable), identity) ||
                !authorizer.canDropResource(createResource(table), identity) ||
                !writeableCatalogs.contains(newTable.getCatalogName()) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyAddColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyRenameColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyInsertTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity) ||
                !writeableCatalogs.contains(table.getCatalogName())) {
            denyDeleteTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        if (!authorizer.canCreateResource(createResource(view), identity) ||
                !writeableCatalogs.contains(view.getCatalogName())) {
            denyCreateView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        if (!authorizer.canDropResource(createResource(view), identity) ||
                !writeableCatalogs.contains(view.getCatalogName())) {
            denyDropView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName view, Set<String> columns)
    {
        RangerAccessResult accessResult =
                authorizer.checkPermission(createResource(view, StringUtils.join(columns.toArray(), COLUMN_SEP)), identity, PrestoAccessType.SELECT);
        if (!accessResult.getIsAllowed()) {
            denySelectColumns(view.toString(), columns, accessResult.getReason());
        }
    }

    private static RangerPrestoResource createResource(CatalogSchemaName catalogSchema)
    {
        return createResource(catalogSchema.getCatalogName(), catalogSchema.getSchemaName());
    }

    private static RangerPrestoResource createResource(CatalogSchemaTableName catalogSchema)
    {
        return createResource(catalogSchema.getCatalogName(), catalogSchema.getSchemaTableName().getSchemaName(), catalogSchema.getSchemaTableName().getTableName());
    }

    private static RangerPrestoResource createResource(CatalogSchemaTableName catalogSchema, String columns)
    {
        return createResource(catalogSchema.getCatalogName(), catalogSchema.getSchemaTableName().getSchemaName(), catalogSchema.getSchemaTableName().getTableName(), columns);
    }

    private static RangerPrestoResource createResource(final String catalogName, final String schemaName)
    {
        return new RangerPrestoResource(catalogName, Optional.of(schemaName));
    }

    private static RangerPrestoResource createResource(final String catalogName, final String schemaName, final String tableName)
    {
        return new RangerPrestoResource(catalogName, schemaName, Optional.of(tableName));
    }

    private static RangerPrestoResource createResource(final String catalogName, final String schemaName, final String tableName, final String columns)
    {
        return new RangerPrestoResource(catalogName, schemaName, tableName, Optional.of(columns));
    }
}
