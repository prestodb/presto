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
package com.facebook.presto.hive.security.ranger;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class RangerBasedAccessControlConfig
{
    public static final String RANGER_POLICY_REFRESH_PERIOD = "hive.ranger.refresh-policy-period";
    public static final String RANGER_HTTP_END_POINT = "hive.ranger.rest-endpoint";
    public static final String RANGER_REST_POLICY_HIVE_SERVICE_NAME = "hive.ranger.policy.hive-servicename";
    public static final String RANGER_REST_USER_GROUPS_AUTH_USERNAME = "hive.ranger.service.basic-auth-username";
    public static final String RANGER_REST_USER_GROUPS_AUTH_PASSWORD = "hive.ranger.service.basic-auth-password";
    public static final String RANGER_HIVE_AUDIT_PATH = "hive.ranger.audit.path";

    public static final String RANGER_REST_POLICY_MGR_DOWNLOAD_URL = "/service/plugins/policies/download";
    public static final String RANGER_REST_USER_GROUP_URL = "/service/xusers/users";
    public static final String RANGER_REST_USER_ROLES_URL = "/service/roles/roles/user";

    private String rangerHttpEndPoint;
    private String rangerHiveServiceName;
    private Duration refreshPeriod = new Duration(60, TimeUnit.SECONDS);
    private String basicAuthUser;
    private String basicAuthPassword;
    private String rangerHiveAuditPath;

    @MinDuration("60s")
    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }

    @Config(RANGER_POLICY_REFRESH_PERIOD)
    public RangerBasedAccessControlConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }

    @NotNull
    public String getRangerHiveServiceName()
    {
        return rangerHiveServiceName;
    }

    @Config(RANGER_REST_POLICY_HIVE_SERVICE_NAME)
    public RangerBasedAccessControlConfig setRangerHiveServiceName(String rangerHiveServiceName)
    {
        this.rangerHiveServiceName = rangerHiveServiceName;
        return this;
    }

    @NotNull
    public String getRangerHttpEndPoint()
    {
        return rangerHttpEndPoint;
    }

    @Config(RANGER_HTTP_END_POINT)
    public RangerBasedAccessControlConfig setRangerHttpEndPoint(String rangerHttpEndPoint)
    {
        this.rangerHttpEndPoint = rangerHttpEndPoint;
        return this;
    }

    public String getBasicAuthUser()
    {
        return basicAuthUser;
    }

    @Config(RANGER_REST_USER_GROUPS_AUTH_USERNAME)
    public RangerBasedAccessControlConfig setBasicAuthUser(String basicAuthUser)
    {
        this.basicAuthUser = basicAuthUser;
        return this;
    }

    public String getBasicAuthPassword()
    {
        return basicAuthPassword;
    }

    @Config(RANGER_REST_USER_GROUPS_AUTH_PASSWORD)
    public RangerBasedAccessControlConfig setBasicAuthPassword(String basicAuthPassword)
    {
        this.basicAuthPassword = basicAuthPassword;
        return this;
    }

    public String getRangerHiveAuditPath()
    {
        return rangerHiveAuditPath;
    }

    @Config(RANGER_HIVE_AUDIT_PATH)
    public RangerBasedAccessControlConfig setRangerHiveAuditPath(String rangerHiveAuditPath)
    {
        this.rangerHiveAuditPath = rangerHiveAuditPath;
        return this;
    }
}
