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
package com.facebook.presto.plugin.ranger.security.ranger;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;

import java.io.IOException;

public class RangerConnectorAccessFactory
{
    private static final Logger log = Logger.get(RangerConnectorAccessFactory.class);
    private static volatile RangerPrestoPlugin tmpRangerPlugin;

    public String getName()
    {
        return "ranger-access-control";
    }

    public void create(RangerBasedAccessControlConfig config)
            throws IOException
    {
        RangerConfiguration rangerConfig = RangerConfiguration.getInstance();
        try {
            handleKerberos(rangerConfig, config);
        }
        catch (IOException e) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Failed to do kerberos right", e);
        }

        tmpRangerPlugin = new RangerPrestoPlugin("hive", "hive");
        tmpRangerPlugin.init();
    }

    private void handleKerberos(RangerConfiguration rangerConfig, RangerBasedAccessControlConfig config)
            throws IOException
    {
        String principal = config.getPrinciple();
        String keytab = config.getPrinciple();
        if (Strings.isNullOrEmpty(principal) || Strings.isNullOrEmpty(keytab)) {
            log.info("Found no kerberos credentials, not communicating with Ranger via Kerberos");
            return;
        }

        rangerConfig.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(rangerConfig);
        MiscUtil.authWithKerberos(keytab, principal, null);
    }
}
