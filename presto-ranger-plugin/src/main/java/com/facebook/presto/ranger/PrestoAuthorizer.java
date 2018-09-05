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

import com.facebook.presto.spi.security.Identity;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerDataMaskResult;
import org.apache.ranger.plugin.policyengine.RangerRowFilterResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PrestoAuthorizer
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";
    private CatalogPlugin catalogPlugin;
    private UserGroups userGroups;

    public PrestoAuthorizer(UserGroups groups, Map<String, RangerPrestoPlugin> plugins)
    {
        this.catalogPlugin = new CatalogPlugin(plugins);
        this.userGroups = groups;
    }

    public List<RangerPrestoResource> filterResources(List<RangerPrestoResource> resources, Identity identity)
    {
        List<RangerPrestoResource> rangerPrestoResources = new ArrayList<>();
        for (RangerPrestoResource rangerPrestoResource : resources) {
            if (checkPermisionForResource(rangerPrestoResource, identity, PrestoAccessType.USE)) {
                rangerPrestoResources.add(rangerPrestoResource);
            }
        }
        return rangerPrestoResources;
    }

    public boolean canSeeResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermisionForResource(resource, identity, PrestoAccessType.USE);
    }

    public boolean canCreateResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermisionForResource(resource, identity, PrestoAccessType.CREATE);
    }

    public boolean canDropResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermisionForResource(resource, identity, PrestoAccessType.DROP);
    }

    public boolean canUpdateResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermisionForResource(resource, identity, PrestoAccessType.UPDATE);
    }

    public boolean canSelectResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermisionForResource(resource, identity, PrestoAccessType.SELECT);
    }

    private boolean checkPermisionForResource(RangerPrestoResource resource, Identity identity, PrestoAccessType prestoAccessType)
    {
        Optional<RangerAccessResult> rangerPrestoPlugin = checkPermission(resource, identity, prestoAccessType);
        if (rangerPrestoPlugin.isPresent()) {
            if (resource.getSchemaTable().isPresent() && INFORMATION_SCHEMA_NAME.equals(resource.getSchemaTable().get().getSchemaName())) {
                return true;
            }
            return rangerPrestoPlugin.get().getIsAllowed();
        }
        else {
            return true;
        }
    }

    private Optional<RangerAccessResult> checkPermission(RangerPrestoResource resource, Identity identity, PrestoAccessType accessType)
    {
        Optional<RangerPrestoPlugin> rangerPrestoPlugin = catalogPlugin.getPluginForCatalog(resource.getCatalogName());
        if (rangerPrestoPlugin.isPresent()) {
            RangerPrestoAccessRequest rangerRequest = new RangerPrestoAccessRequest(
                    resource,
                    identity.getUser(),
                    getGroups(identity),
                    accessType);

            return Optional.of(rangerPrestoPlugin.get().isAccessAllowed(rangerRequest));
        }
        return Optional.empty();
    }

    private Set<String> getGroups(Identity identity)
    {
        return userGroups.getUserGroups(identity.getUser());
    }

    public String getRowLevelFilterExp(String catalogName, RangerPrestoResource resource, Identity identity)
    {
        Optional<RangerPrestoPlugin> rangerPrestoPlugin = catalogPlugin.getPluginForCatalog(catalogName);
        if (rangerPrestoPlugin.isPresent()) {
            RangerPrestoAccessRequest rangerRequest = new RangerPrestoAccessRequest(
                    resource,
                    identity.getUser(),
                    getGroups(identity),
                    PrestoAccessType.SELECT);

            RangerRowFilterResult rowFilterResult = rangerPrestoPlugin.get().evalRowFilterPolicies(rangerRequest, rangerPrestoPlugin.get().getResultProcessor());
            if (isRowFilterEnabled(rowFilterResult)) {
                return rowFilterResult.getFilterExpr();
            }
        }
        return null;
    }

    public String getColumnMaskingExpression(String catalogName, RangerPrestoResource resource, Identity identity, String columnName)
    {
        Optional<RangerPrestoPlugin> rangerPrestoPlugin = catalogPlugin.getPluginForCatalog(catalogName);
        if (rangerPrestoPlugin.isPresent()) {
            RangerPrestoAccessRequest rangerRequest = new RangerPrestoAccessRequest(
                    resource,
                    identity.getUser(),
                    getGroups(identity),
                    PrestoAccessType.SELECT);

            RangerDataMaskResult rangerDataMaskResult = rangerPrestoPlugin.get().evalDataMaskPolicies(rangerRequest, rangerPrestoPlugin.get().getResultProcessor());
            if (isDataMaskEnabled(rangerDataMaskResult)) {
                // only support for custom masking
                if (StringUtils.equalsIgnoreCase(rangerDataMaskResult.getMaskType(), RangerPolicy.MASK_TYPE_CUSTOM)) {
                    String maskedValue = rangerDataMaskResult.getMaskedValue();
                    if (maskedValue == null) {
                        return "NULL";
                    }
                    else {
                        return maskedValue.replace("{col}", columnName);
                    }
                }
            }
        }

        return null;
    }

    private boolean isDataMaskEnabled(RangerDataMaskResult result)
    {
        return result != null && result.isMaskEnabled() && !StringUtils.equalsIgnoreCase(result.getMaskType(), RangerPolicy.MASK_TYPE_NONE);
    }

    private boolean isRowFilterEnabled(RangerRowFilterResult result)
    {
        return result != null && result.isRowFilterEnabled() && StringUtils.isNotEmpty(result.getFilterExpr());
    }

    private class CatalogPlugin
    {
        private Map<String, RangerPrestoPlugin> plugins;

        public CatalogPlugin(Map<String, RangerPrestoPlugin> plugins)
        {
            this.plugins = plugins;
        }

        public Optional<RangerPrestoPlugin> getPluginForCatalog(String catalogName)
        {
            return Optional.ofNullable(plugins.get(catalogName));
        }
    }
}
