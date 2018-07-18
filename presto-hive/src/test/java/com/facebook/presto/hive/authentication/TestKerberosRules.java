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
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import sun.security.krb5.Config;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import static org.apache.hadoop.security.UserGroupInformationShim.createUserGroupInformationForSubject;
import static org.apache.hadoop.security.authentication.util.KerberosName.resetDefaultRealm;
import static org.testng.Assert.assertEquals;
import static java.util.Collections.emptySet;

public class TestKerberosRules
{
    private String originalConf;

    @BeforeClass
    public void setup()
    {
        originalConf = System.getProperty("java.security.krb5.conf");

        // default realm in this file is REALM1.COM
        System.setProperty("java.security.krb5.conf", "src/test/resources/mock-krb5.conf");
    }

    public String tryExtractingUserName(String principalName, HdfsConfiguration hdfsConfiguration) throws Exception
    {
        KerberosHadoopAuthentication kerberosHadoopAuthentication = new KerberosHadoopAuthentication(new KerberosAuthentication(principalName), hdfsConfiguration);
        KerberosPrincipal principal = new KerberosPrincipal(principalName);
        Subject subject = new Subject(false, ImmutableSet.of(principal), emptySet(), emptySet());
        UserGroupInformation ugi = createUserGroupInformationForSubject(subject);
        String shortName = ugi.getShortUserName();
        UserGroupInformation.reset();
        return shortName;
    }

    public String testUserNameExtraction(String resourceConfigFile, String principalName) throws Exception
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig().setResourceConfigFiles(resourceConfigFile);
        HdfsConfigurationUpdater updater = new HdfsConfigurationUpdater(hiveClientConfig);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updater);
        return tryExtractingUserName(principalName, hdfsConfiguration);
    }

    @Test
    public void testSameRealmWithoutRules() throws Exception
    {
        String shortName = testUserNameExtraction("src/test/resources/mock-core-site-without-rules.xml", "presto@REALM1.COM");
        assertEquals(shortName, "presto");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDifferentRealmWithoutRules() throws Exception
    {
        testUserNameExtraction("src/test/resources/mock-core-site-without-rules.xml", "presto@REALM2.COM");
    }

    @Test
    public void testDifferentRealmWithRules() throws Exception
    {
        String shortName = testUserNameExtraction("src/test/resources/mock-core-site-with-rules.xml", "presto@REALM2.COM");
        assertEquals(shortName, "presto");
    }

    @AfterClass
    public void restore() throws Exception
    {
        if (originalConf == null) {
            System.clearProperty("java.security.krb5.conf");
        }
        else {
            System.setProperty("java.security.krb5.conf", originalConf);
        }
        Config.refresh();
        resetDefaultRealm();
    }
}
