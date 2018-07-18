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
package com.facebook.presto.hive.authentication;

import com.facebook.presto.hive.HdfsConfiguration;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;

import javax.security.auth.Subject;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.UserGroupInformationShim.createUserGroupInformationForSubject;

public class KerberosHadoopAuthentication
        implements HadoopAuthentication
{
    private static final Logger log = Logger.get(KerberosHadoopAuthentication.class);

    private final KerberosAuthentication kerberosAuthentication;

    public KerberosHadoopAuthentication(KerberosAuthentication kerberosAuthentication, HdfsConfiguration hdfsConfiguration)
    {
        this.kerberosAuthentication = requireNonNull(kerberosAuthentication, "kerberosAuthentication is null");
        requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        Configuration configuration = validateHDFSConfiguration(hdfsConfiguration);
        UserGroupInformation.setConfiguration(configuration);
    }

    @Override
    public UserGroupInformation getUserGroupInformation()
    {
        Subject subject = kerberosAuthentication.getSubject();
        return createUserGroupInformationForSubject(subject);
    }

    private Configuration validateHDFSConfiguration(HdfsConfiguration hdfsConfiguration)
    {
        Configuration config = hdfsConfiguration.getConfiguration(null, null);
        String loadedHadoopAuthentication = config.get("hadoop.security.authentication");

        if (!"kerberos".equals(loadedHadoopAuthentication)) {
            log.warn("Contradicting security authentication type in hive.properties and hive config resources");
            log.warn("Setting security authentication type to kerberos according to hive.properties");
            config.set("hadoop.security.authentication", "kerberos");
        }

        String authToLocalRules = config.get("hadoop.security.auth_to_local");
        if (authToLocalRules == null) {
            KerberosName.setRules("DEFAULT");
        }
        return config;
    }
}
