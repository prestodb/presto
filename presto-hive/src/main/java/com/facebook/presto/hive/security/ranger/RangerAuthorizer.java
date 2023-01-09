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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_RANGER_SERVER_ERROR;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RangerAuthorizer
{
    private static final Logger log = Logger.get(RangerAuthorizer.class);
    private static final String KEY_DATABASE = "database";
    private static final String KEY_TABLE = "table";
    private static final String KEY_COLUMN = "column";
    private static final String CLUSTER_NAME = "Presto";
    private static final String HIVE = "hive";

    private final RangerBasePlugin plugin;
    private final Supplier<ServicePolicies> servicePolicies;
    private final AtomicReference<ServicePolicies> currentServicePolicies = new AtomicReference<>();

    public RangerAuthorizer(Supplier<ServicePolicies> servicePolicies, RangerBasedAccessControlConfig rangerBasedAccessControlConfig)
    {
        this.servicePolicies = requireNonNull(servicePolicies, "ServicePolicies is null");
        RangerPolicyEngineOptions rangerPolicyEngineOptions = new RangerPolicyEngineOptions();
        Configuration conf = new Configuration();
        rangerPolicyEngineOptions.configureDefaultRangerAdmin(conf, "hive");
        RangerPluginConfig rangerPluginConfig = new RangerPluginConfig(HIVE, rangerBasedAccessControlConfig.getRangerHiveServiceName(), HIVE, CLUSTER_NAME, null,
                rangerPolicyEngineOptions);
        plugin = new RangerBasePlugin(rangerPluginConfig);

        String hiveAuditPath = rangerBasedAccessControlConfig.getRangerHiveAuditPath();
        if (!isNullOrEmpty(hiveAuditPath)) {
            try {
                plugin.getConfig().addResource(new File(hiveAuditPath).toURI().toURL());
            }
            catch (MalformedURLException e) {
                log.error(e, "Invalid audit file is provided ");
            }
        }

        AuditProviderFactory providerFactory = AuditProviderFactory.getInstance();

        if (!providerFactory.isInitDone()) {
            if (plugin.getConfig().getProperties() != null) {
                providerFactory.init(plugin.getConfig().getProperties(), HIVE);
            }
            else {
                log.info("Audit subsystem is not initialized correctly. Please check audit configuration. ");
                log.info("No authorization audits will be generated. ");
            }
        }

        plugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    private void updateRangerPolicies()
    {
        ServicePolicies newServicePolicies = getRangerServicePolicies();
        ServicePolicies existingServicePolicies = currentServicePolicies.get();
        if (newServicePolicies != existingServicePolicies && currentServicePolicies.compareAndSet(existingServicePolicies, newServicePolicies)) {
            plugin.setPolicies(newServicePolicies);
        }
    }

    private ServicePolicies getRangerServicePolicies()
    {
        try {
            return servicePolicies.get();
        }
        catch (Exception ex) {
            throw new PrestoException(HIVE_RANGER_SERVER_ERROR, "Unable to fetch policy information from ranger", ex);
        }
    }

    public boolean authorizeHiveResource(String database, String table, String column, String accessType, String user, Set<String> userGroups, Set<String> userRoles)
    {
        updateRangerPolicies();
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        if (!isNullOrEmpty(database)) {
            resource.setValue(KEY_DATABASE, database);
        }

        if (!isNullOrEmpty(table)) {
            resource.setValue(KEY_TABLE, table);
        }

        if (!isNullOrEmpty(column)) {
            resource.setValue(KEY_COLUMN, column);
        }

        RangerAccessRequest request = new RangerAccessRequestImpl(resource, accessType.toLowerCase(ENGLISH), user, userGroups, userRoles);

        RangerAccessResult result = plugin.isAccessAllowed(request);

        return result != null && result.getIsAllowed();
    }
}
