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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.uri.UriComponent;
import io.airlift.log.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.RangerRESTUtils;

import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class UserGroups
{
    private static final Logger log = Logger.get(UserGroups.class);
    private static final String GET_USER_INFO = "/service/xusers/users/userName/%s";
    private static final String GET_USER_GROUPS = "/service/xusers/%d/groups";
    private static final String KERBEROS = "kerberos";
    private final RangerRESTClient restClient;
    private final LoadingCache<String, Set<String>> userGroupCache;
    private final boolean isKerberos;

    public UserGroups(Map<String, String> configuration)
    {
        this.restClient = new RangerRESTClient();
        this.isKerberos =
                KERBEROS.equalsIgnoreCase(RangerConfiguration.getInstance().get("hadoop.security.authentication"));
        restClient.setBasicAuthInfo(configuration.get("ranger.username"), configuration.get("ranger.password"));
        int cacheExpiry = Integer.parseInt(configuration.getOrDefault("user-group-cache-expiry-ttl", "30"));
        this.userGroupCache = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiry, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Set<String>>()
                {
                    @Override
                    public Set<String> load(String key)
                            throws Exception
                    {
                        return loadUserGroups(key);
                    }
                });
    }

    public Set<String> getUserGroups(String username)
    {
        try {
            return userGroupCache.get(username);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> loadUserGroups(String username)
    {
        Optional<Integer> userId = getUserId(username);
        return userId.map(this::groupsForUser).orElse(ImmutableSet.of());
    }

    private ClientResponse makeUserInfoRequest(String user)
    {
        WebResource resource = restClient.getResource(
                String.format(GET_USER_INFO, UriComponent.contextualEncode(user, UriComponent.Type.PATH_SEGMENT)));
        return resource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).type(RangerRESTUtils.REST_MIME_TYPE_JSON)
                .get(ClientResponse.class);
    }

    private Optional<Integer> getUserId(String userName)
    {
        try {
            ClientResponse response = doRequest(() -> makeUserInfoRequest(userName));
            if (response.getStatus() == HttpServletResponse.SC_OK) {
                RangerUserInfo entity = (RangerUserInfo) response.getEntity(RangerUserInfo.class);
                return Optional.of(entity.getId());
            }
            String errorMsg = response.hasEntity() ? response.getEntity(String.class) : null;
            log.error("Error getting user id for  userName=" + userName + ", response=" + response.getStatus()
                    + ", errorMsg=" + errorMsg);
            return Optional.empty();
        }
        catch (Throwable t) {
            log.error(t, "Error getting userid for: " + userName);
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, t);
        }
    }

    private ClientResponse doRequest(Supplier<ClientResponse> f)
    {
        try {
            UserGroupInformation user = MiscUtil.getLoginUser();
            if (user != null && isKerberos) {
                return user.doAs((PrivilegedAction<ClientResponse>) () -> f.get());
            }
            else {
                return f.get();
            }
        }
        catch (IOException e) {
            log.error("Error performing request.", e);
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
        }
    }

    private ClientResponse makeUserGroupsRequest(int userId)
    {
        WebResource resource = restClient.getResource(String.format(GET_USER_GROUPS, userId));
        ClientResponse response = resource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
        return response;
    }

    private Set<String> groupsForUser(int userId)
    {
        try {
            ClientResponse response = doRequest(() -> makeUserGroupsRequest(userId));
            if (response.getStatus() == HttpServletResponse.SC_OK) {
                RangerUserGroups entity = response.getEntity(RangerUserGroups.class);
                return entity.getvXGroups().stream().map(RangerGroup::getName).collect(Collectors.toSet());
            }
            String errorMsg = response.hasEntity() ? response.getEntity(String.class) : null;
            log.error(
                    "Error getting groups for  userId=" + userId + ", response=" + response.getStatus() + ", errorMsg="
                            + errorMsg);
            return ImmutableSet.of();
        }
        catch (Throwable t) {
            log.error(t, "Error getting groups for user id: " + userId);
            throw t;
        }
    }
}
