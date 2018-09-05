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
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.spi.security.AccessDeniedException.denyAddColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCatalogAccess;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDeleteTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denyInsertTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameColumn;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameSchema;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRenameTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectColumns;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetUser;
import static com.facebook.presto.spi.security.AccessDeniedException.denyShowSchemas;
import static com.facebook.presto.spi.security.AccessDeniedException.denyShowTablesMetadata;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class RangerSystemAccessControl
        implements SystemAccessControl
{
    private final Set<String> powerPrincipals;
    private final PrestoAuthorizer authorizer;

    public RangerSystemAccessControl(PrestoAuthorizer prestoAuthorizer, Map<String, String> config)
    {
        this.authorizer = prestoAuthorizer;

        String[] powerPrincipals = config.getOrDefault("power-principals", "").split(",");
        this.powerPrincipals = Arrays.stream(powerPrincipals).filter(s -> !s.isEmpty()).map(s -> s.toLowerCase(ENGLISH)).collect(toSet());
    }

    private static RangerPrestoResource createResource(CatalogSchemaName catalogSchema)
    {
        return createResource(catalogSchema.getCatalogName(), catalogSchema.getSchemaName());
    }

    private static RangerPrestoResource createResource(CatalogSchemaTableName catalogSchema)
    {
        return createResource(catalogSchema.getCatalogName(), catalogSchema.getSchemaTableName().getSchemaName(),
                catalogSchema.getSchemaTableName().getTableName());
    }

    private static RangerPrestoResource createResource(String catalogName)
    {
        return new RangerPrestoResource(catalogName, Optional.empty(), Optional.empty());
    }

    private static RangerPrestoResource createResource(String catalogName, String schemaName)
    {
        return new RangerPrestoResource(catalogName, Optional.of(schemaName), Optional.empty());
    }

    private static RangerPrestoResource createResource(String catalogName, String schemaName, final String tableName)
    {
        return new RangerPrestoResource(catalogName, Optional.of(schemaName), Optional.of(tableName));
    }

    private static List<RangerPrestoResource> createResource(CatalogSchemaTableName table, Set<String> columns)
    {
        List<RangerPrestoResource> colRequests = new ArrayList<>();

        if (columns.size() > 0) {
            for (String column : columns) {
                RangerPrestoResource rangerPrestoResource = new RangerPrestoResource(table.getCatalogName(), Optional.of(table.getSchemaTableName().getSchemaName()), Optional.of(table.getSchemaTableName().getTableName()), Optional.of(column));
                colRequests.add(rangerPrestoResource);
            }
        }
        else {
            colRequests.add(new RangerPrestoResource(table.getCatalogName(), Optional.of(table.getSchemaTableName().getSchemaName()), Optional.of(table.getSchemaTableName().getTableName()), Optional.empty()));
        }
        return colRequests;
    }

    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        if (principal == null) {
            return;
        }
        if (powerPrincipals.contains(principal.getName().toLowerCase(ENGLISH))) {
            return;
        }
        String principalName = principal.getName().replaceAll("@.*", "").replaceAll("/.*", "");
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
        denySetSystemSessionProperty(propertyName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        denySetSystemSessionProperty(propertyName);
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
        denySetSystemSessionProperty("");
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table,
            String revokee, boolean grantOptionFor)
    {
        denyRevokeTablePrivilege(table.getCatalogName(), table.getSchemaTableName().getSchemaName(), table.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!authorizer.canSelectResource(new RangerPrestoResource(table.getCatalogName(),
                Optional.of(table.getSchemaTableName().getSchemaName()), Optional.of(table.getSchemaTableName().getTableName()), columns), identity)) {
            denySelectColumns(table.getSchemaTableName().getTableName(), columns);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table,
            Set<String> columns)
    {
        if (!authorizer.canCreateResource(createResource(table), identity)) {
            denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), identity);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName catalogSchemaTableName,
            String grantee, boolean withGrantOption)
    {
        denyGrantTablePrivilege(catalogSchemaTableName.getCatalogName(), catalogSchemaTableName.getSchemaTableName().getSchemaName(), catalogSchemaTableName.getSchemaTableName().getTableName());
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        if (!authorizer.canSeeResource(createResource(catalogName), identity)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        if (!authorizer.canSeeResource(createResource(catalogName), identity)) {
            denyShowSchemas(catalogName);
        }
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName catalogSchemaName)
    {
        if (!authorizer.canSeeResource(createResource(catalogSchemaName.getCatalogName(), catalogSchemaName.getSchemaName()), identity)) {
            denyShowTablesMetadata(catalogSchemaName.getSchemaName());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        List<RangerPrestoResource> rangerResources = tableNames.stream()
                .map(t -> new RangerPrestoResource(catalogName, Optional.of(t.getSchemaName()), Optional.of(t.getTableName())))
                .collect(Collectors.toList());

        Stream<SchemaTableName> outTables = authorizer.filterResources(rangerResources, identity).stream()
                .map(RangerPrestoResource::getSchemaTable).filter(schemaTableName -> schemaTableName.isPresent()).map(schemaTableName -> schemaTableName.get());

        return makeSortedSet(outTables, comparing(t -> t.toString().toLowerCase(ENGLISH)));
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        List<RangerPrestoResource> rangerResources = schemaNames.stream()
                .map(schemaName -> new RangerPrestoResource(catalogName, Optional.of(schemaName), Optional.empty()))
                .collect(toList());

        Stream<String> outSchemas =
                authorizer.filterResources(rangerResources, identity).stream().map(RangerPrestoResource::getDatabase);

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
        if (!authorizer.canCreateResource(createResource(schema), identity)) {
            denyCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!authorizer.canCreateResource(createResource(schema), identity)) {
            denyDropSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName catalogSchema, String newSchemaName)
    {
        if (!authorizer.canCreateResource(createResource(catalogSchema), identity) || !authorizer.canDropResource(
                createResource(catalogSchema.getCatalogName(), catalogSchema.getSchemaName(), newSchemaName), identity)) {
            denyRenameSchema(catalogSchema.getSchemaName(), newSchemaName);
        }
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canCreateResource(createResource(table), identity)) {
            denyCreateTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canDropResource(createResource(table), identity)) {
            denyDropTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!authorizer.canCreateResource(createResource(newTable), identity) || !authorizer
                .canDropResource(createResource(table), identity)) {
            denyRenameTable(table.getSchemaTableName().getTableName(), newTable.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity)) {
            denyAddColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity)) {
            denyRenameColumn(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity)) {
            denyInsertTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!authorizer.canUpdateResource(createResource(table), identity)) {
            denyDeleteTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        if (!authorizer.canCreateResource(createResource(view), identity)) {
            denyCreateView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        if (!authorizer.canDropResource(createResource(view), identity)) {
            denyDropView(view.getSchemaTableName().getTableName());
        }
    }

    @Override
    public String applyRowLevelFiltering(Identity identity, CatalogSchemaTableName table)
    {
        RangerPrestoResource rp = new RangerPrestoResource(table.getCatalogName(),
                Optional.of(table.getSchemaTableName().getSchemaName()), Optional.of(table.getSchemaTableName().getTableName()));
        return authorizer.getRowLevelFilterExp(table.getCatalogName(), rp, identity);
    }

    @Override
    public String applyColumnMasking(Identity identity, CatalogSchemaTableName table, String columnName)
    {
        RangerPrestoResource rp = new RangerPrestoResource(table.getCatalogName(),
                Optional.of(table.getSchemaTableName().getSchemaName()), Optional.of(table.getSchemaTableName().getTableName()), Optional.of(columnName));
        return authorizer.getColumnMaskingExpression(table.getCatalogName(), rp, identity, columnName);
    }
}
