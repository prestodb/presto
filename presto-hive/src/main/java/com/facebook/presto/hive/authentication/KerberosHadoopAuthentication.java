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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;

import javax.security.auth.Subject;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.security.UserGroupInformationShim.createUserGroupInformationForSubject;

public class KerberosHadoopAuthentication
        implements HadoopAuthentication
{
    static {
        // In order to enable KERBEROS authentication method for HDFS
        // UserGroupInformation.authenticationMethod static field must be set to KERBEROS
        // It is further used in many places in DfsClient
        Configuration configuration = new Configuration(false);
        configuration.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(configuration);

        // KerberosName#rules static field must be initialized
        // It is used in KerberosName#getShortName which is used in User constructor invoked by UserGroupInformation#getUGIFromSubject
        KerberosName.setRules("DEFAULT");
    }

    private final KerberosAuthentication kerberosAuthentication;

    public KerberosHadoopAuthentication(KerberosAuthentication kerberosAuthentication)
    {
        this.kerberosAuthentication = requireNonNull(kerberosAuthentication, "kerberosAuthentication is null");
    }

    @Override
    public UserGroupInformation getUserGroupInformation()
    {
        Subject subject = kerberosAuthentication.getSubject();
        return createUserGroupInformationForSubject(subject);
    }
}
