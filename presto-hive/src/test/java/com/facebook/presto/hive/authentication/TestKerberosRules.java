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
import com.facebook.presto.hive.HdfsConfigurationUpdater;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestKerberosRules
{
    public void setRules(Configuration configuration)
    {
        String authToLocalRules = configuration.get("hadoop.security.auth_to_local");
        if (authToLocalRules == null) {
            KerberosName.setRules("DEFAULT");
        }
        UserGroupInformation.setConfiguration(configuration);
    }

    @Test
    public void tesKerberosRulesLoaded()
    {
        String resourcePath = "src/test/resources/mock-core-site-with-rules.xml";
        Configuration configuration = getConfiguration(resourcePath);
        setRules(configuration);
        checkKerberosRulesLoaded(resourcePath);
    }

    private Configuration getConfiguration(String resourcePath)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig().setResourceConfigFiles(resourcePath);
        HdfsConfigurationUpdater updater = new HdfsConfigurationUpdater(hiveClientConfig);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updater);

        Configuration configuration = hdfsConfiguration.getConfiguration(null, null);
        configuration.set("hadoop.security.authentication", "kerberos");
        return configuration;
    }

    private void checkKerberosRulesLoaded(String resourcePath)
    {
        Configuration resourceProperties = new Configuration(false);
        resourceProperties.addResource(new Path(resourcePath));
        assertEquals(resourceProperties.get("hadoop.security.auth_to_local").trim(), KerberosName.getRules().trim());
    }
}
