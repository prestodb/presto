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
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;

import java.io.IOException;
import java.util.Map;

public class RangerSystemAccessControlFactory
        implements SystemAccessControlFactory
{
    private static volatile RangerPrestoPlugin rangerPlugin;
    private static final Logger log = Logger.get(RangerSystemAccessControlFactory.class);

    @Override
    public String getName()
    {
        return "ranger-access-control";
    }

    @Override
    public SystemAccessControl create(Map<String, String> config)
    {
        RangerConfiguration rangerConfig = RangerConfiguration.getInstance();
        try {
            handleKerberos(rangerConfig, config);
        }
        catch (IOException e) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Failed to do kerberos right", e);
        }

        for (final Map.Entry<String, String> configEntry : config.entrySet()) {
            if (configEntry.getKey().startsWith("ranger.")) {
                rangerConfig.set(configEntry.getKey(), configEntry.getValue());
                log.info("Setting: " + configEntry.getKey() + " to: " + configEntry.getValue());
            }
        }

        PrestoAuthorizer authorizer = getPrestoAuthorizer(config);
        return new RangerSystemAccessControl(authorizer, config);
    }

    private void handleKerberos(RangerConfiguration rangerConfig, Map<String, String> config)
            throws IOException
    {
        String principal = config.get("login-to-ranger-principal");
        String keytab = config.get("login-to-ranger-keytab");
        if (Strings.isNullOrEmpty(principal) || Strings.isNullOrEmpty(keytab)) {
            log.info("Found no kerberos credentials, not communicating with Ranger via Kerberos");
            return;
        }

        rangerConfig.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(rangerConfig);
        MiscUtil.authWithKerberos(keytab, principal, null);
    }

    private PrestoAuthorizer getPrestoAuthorizer(Map<String, String> configuration)
    {
        if (rangerPlugin == null) {
            synchronized (RangerSystemAccessControlFactory.class) {
                if (rangerPlugin == null) {
                    RangerPrestoPlugin tmpRangerPlugin = new RangerPrestoPlugin("presto", "presto");
                    tmpRangerPlugin.init();
                    rangerPlugin = tmpRangerPlugin;
                }
            }
        }

        UserGroups userGroups = new UserGroups(configuration);

        return new PrestoAuthorizer(userGroups, rangerPlugin);
    }
}
