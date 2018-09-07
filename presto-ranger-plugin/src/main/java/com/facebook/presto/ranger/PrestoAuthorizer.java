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
import io.airlift.log.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PrestoAuthorizer
{
    private final Logger log = Logger.get(PrestoAuthorizer.class);
    private final RangerPrestoPlugin plugin;
    private UserGroups userGroups;

    public PrestoAuthorizer(UserGroups groups, RangerPrestoPlugin plugin)
    {
        this.plugin = plugin;
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

    public boolean canSelectOnResource(RangerPrestoResource resource, Identity identity)
    {
        return checkPermission(resource, identity, PrestoAccessType.SELECT).getIsAllowed();
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

        return plugin.isAccessAllowed(rangerRequest);
    }

    private Set<String> getGroups(Identity identity)
    {
        return userGroups.getUserGroups(identity.getUser());
    }
}
