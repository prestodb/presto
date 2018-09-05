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
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerRowFilterResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PrestoAuthorizer
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";
    private Map<String, RangerPrestoPlugin> plugins;
    private UserGroups userGroups;

    public PrestoAuthorizer(UserGroups groups, Map<String, RangerPrestoPlugin> plugins)
    {
        this.plugins = plugins;
        this.userGroups = groups;
    }

    public List<RangerPrestoResource> filterResources(List<RangerPrestoResource> resources, Identity identity)
    {
        return resources.stream()
                .map(resource -> checkPermission(resource, identity, PrestoAccessType.USE))
                .filter(RangerAccessResult::getIsAllowed)
                .map(RangerAccessResult::getAccessRequest)
                .map(RangerAccessRequest::getResource)
                .map(resource -> (RangerPrestoResource) resource)
                .collect(Collectors.toList());
    }

    public boolean canSeeResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermission(resource, identity, PrestoAccessType.USE).getIsAllowed();
    }

    public boolean canCreateResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermission(resource, identity, PrestoAccessType.CREATE).getIsAllowed();
    }

    public boolean canDropResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermission(resource, identity, PrestoAccessType.DROP).getIsAllowed();
    }

    public boolean canUpdateResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermission(resource, identity, PrestoAccessType.UPDATE).getIsAllowed();
    }

    private RangerAccessResult checkPermission(RangerPrestoResource resource, Identity identity, PrestoAccessType accessType)
    {
        RangerPrestoAccessRequest rangerRequest = new RangerPrestoAccessRequest(
                resource,
                identity.getUser(),
                getGroups(identity),
                accessType);

        return plugins.get(resource.getCatalogName()).isAccessAllowed(rangerRequest);
    }

    private Set<String> getGroups(Identity identity)
    {
        return userGroups.getUserGroups(identity.getUser());
    }

    public boolean canSelectFromColumns(String catalogName, RangerPrestoResource resource, Identity identity)
    {
        if (INFORMATION_SCHEMA_NAME.equals(resource.getSchemaTable().getSchemaName())) {
            return true;
        }
        RangerPrestoAccessRequest rangerRequest = new RangerPrestoAccessRequest(
                resource,
                identity.getUser(),
                getGroups(identity),
                PrestoAccessType.SELECT);

        RangerAccessResult colResult = plugins.get(catalogName).isAccessAllowed(rangerRequest);
        return colResult != null && colResult.getIsAllowed();
    }

    public boolean canSelectFromColumns(String catalogName, List<RangerPrestoResource> resources, Identity identity)
    {
        RangerAccessResult result = null;
        Collection<RangerAccessRequest> colRequests = new ArrayList<>();

        for (RangerPrestoResource resource : resources) {
            RangerPrestoAccessRequest rangerRequest = new RangerPrestoAccessRequest(
                    resource,
                    identity.getUser(),
                    getGroups(identity),
                    PrestoAccessType.SELECT);
            colRequests.add(rangerRequest);
        }
        Collection<RangerAccessResult> colResults = plugins.get(catalogName).isAccessAllowed(colRequests);
        if (colResults != null) {
            for (RangerAccessResult colResult : colResults) {
                result = colResult;
                if (result != null && !result.getIsAllowed()) {
                    break;
                }
            }
        }
        return result.getIsAllowed();
    }

    public String getRowLevelFilterExp(String catalogName, RangerPrestoResource resource, Identity identity)
    {

        RangerPrestoAccessRequest rangerRequest = new RangerPrestoAccessRequest(
                resource,
                identity.getUser(),
                getGroups(identity),
                PrestoAccessType.SELECT);

        RangerRowFilterResult rowFilterResult = plugins.get(catalogName).evalRowFilterPolicies(rangerRequest, null);
        if (rowFilterResult != null && rowFilterResult.isRowFilterEnabled()) {
            return rowFilterResult.getFilterExpr();
        }
        else {
            return null;
        }
    }
}
