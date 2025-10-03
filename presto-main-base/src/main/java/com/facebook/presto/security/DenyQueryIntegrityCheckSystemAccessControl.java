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
package com.facebook.presto.security;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;

import java.security.Principal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.security.AccessDeniedException.denyQueryIntegrityCheck;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DenyQueryIntegrityCheckSystemAccessControl
        implements SystemAccessControl
{
    public static final String NAME = "deny-query-integrity-check";

    private static final DenyQueryIntegrityCheckSystemAccessControl INSTANCE = new DenyQueryIntegrityCheckSystemAccessControl();

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");
            checkArgument(config.isEmpty(), "This access controller does not support any configuration properties");
            return INSTANCE;
        }
    }

    @Override
    public void checkQueryIntegrity(Identity identity, AccessControlContext context, String query, Map<QualifiedObjectName, ViewDefinition> viewDefinitions, Map<QualifiedObjectName, MaterializedViewDefinition> materializedViewDefinitions)
    {
        denyQueryIntegrityCheck();
    }

    @Override
    public void checkCanSetUser(Identity identity, AccessControlContext context, Optional<Principal> principal, String userName)
    {
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, AccessControlContext context, String propertyName)
    {
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, AccessControlContext context, String catalogName)
    {
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, AccessControlContext context, Set<String> catalogs)
    {
        return catalogs;
    }

    @Override
    public Set<String> filterSchemas(Identity identity, AccessControlContext context, String catalogName, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanShowCreateTable(Identity identity, AccessControlContext context, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, AccessControlContext context, CatalogSchemaTableName table, Set<String> columns)
    {
    }
}
