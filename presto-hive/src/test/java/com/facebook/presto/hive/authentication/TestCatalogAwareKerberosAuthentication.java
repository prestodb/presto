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

import com.facebook.presto.hive.CatalogAwareHdfsConfigurationInitializer;
import com.facebook.presto.hive.CatalogKerberosConfig;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveClientConfig.HdfsAuthenticationType;
import com.facebook.presto.hive.hdfs.HdfsConfigurationInitializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestCatalogAwareKerberosAuthentication
{
    private CatalogAwareKerberosHadoopAuthentication authentication;
    private CatalogAwareHdfsConfigurationInitializer configInitializer;
    private CatalogKerberosConfig catalogConfig;
    private HiveClientConfig hiveConfig;

    @BeforeMethod
    public void setUp()
    {
        hiveConfig = new HiveClientConfig();
        hiveConfig.setHdfsAuthenticationType(HdfsAuthenticationType.KERBEROS);
        hiveConfig.setCatalogAwareKerberosEnabled(true);

        catalogConfig = new CatalogKerberosConfig();
        catalogConfig.setEnableCatalogAwareKerberos(true);

        configInitializer = new CatalogAwareHdfsConfigurationInitializer(
                new HdfsConfigurationInitializer(hiveConfig),
                catalogConfig);

        authentication = new CatalogAwareKerberosHadoopAuthentication(
                configInitializer,
                catalogConfig);
    }

    @Test
    public void testCatalogSpecificAuthToLocalRules()
    {
        // 设置不同catalog的auth_to_local规则
        String catalog1 = "hive_cluster1";
        String catalog2 = "hive_cluster2";
        String authToLocalRule1 = "RULE:[1:$1@$0](.*@REALM1.COM)s/@.*//";
        String authToLocalRule2 = "RULE:[1:$1@$0](.*@REALM2.COM)s/@.*//";

        catalogConfig.addCatalogAuthToLocalRule(catalog1, authToLocalRule1);
        catalogConfig.addCatalogAuthToLocalRule(catalog2, authToLocalRule2);

        // 验证每个catalog都有独立的配置
        Configuration config1 = authentication.getHadoopConfiguration(catalog1);
        Configuration config2 = authentication.getHadoopConfiguration(catalog2);

        assertNotNull(config1);
        assertNotNull(config2);

        // 验证auth_to_local规则被正确设置
        assertEquals(config1.get("hadoop.security.auth_to_local"), authToLocalRule1);
        assertEquals(config2.get("hadoop.security.auth_to_local"), authToLocalRule2);
    }

    @Test
    public void testKerberosNameIsolation()
    {
        String catalog1 = "hive_cluster1";
        String catalog2 = "hive_cluster2";
        String authToLocalRule1 = "RULE:[1:$1@$0](.*@REALM1.COM)s/@.*//";
        String authToLocalRule2 = "RULE:[1:$1@$0](.*@REALM2.COM)s/@.*//";

        catalogConfig.addCatalogAuthToLocalRule(catalog1, authToLocalRule1);
        catalogConfig.addCatalogAuthToLocalRule(catalog2, authToLocalRule2);

        // 获取每个catalog的KerberosName实例
        KerberosName kerberosName1 = authentication.getKerberosNameForCatalog(catalog1);
        KerberosName kerberosName2 = authentication.getKerberosNameForCatalog(catalog2);

        assertNotNull(kerberosName1);
        assertNotNull(kerberosName2);

        // 验证KerberosName实例是独立的
        assertTrue(kerberosName1 != kerberosName2);
    }

    @Test
    public void testCatalogConfigurationUpdate()
    {
        String catalogName = "test_catalog";
        String initialRule = "RULE:[1:$1@$0](.*@INITIAL.COM)s/@.*//";
        String updatedRule = "RULE:[1:$1@$0](.*@UPDATED.COM)s/@.*//";

        // 初始设置
        catalogConfig.addCatalogAuthToLocalRule(catalogName, initialRule);
        Configuration initialConfig = authentication.getHadoopConfiguration(catalogName);
        assertEquals(initialConfig.get("hadoop.security.auth_to_local"), initialRule);

        // 更新规则
        catalogConfig.updateCatalogAuthToLocalRule(catalogName, updatedRule);
        Configuration updatedConfig = authentication.getHadoopConfiguration(catalogName);
        assertEquals(updatedConfig.get("hadoop.security.auth_to_local"), updatedRule);
    }

    @Test
    public void testMultipleCatalogsConcurrentAccess()
    {
        Map<String, String> catalogRules = new HashMap<>();
        catalogRules.put("catalog1", "RULE:[1:$1@$0](.*@REALM1.COM)s/@.*//");
        catalogRules.put("catalog2", "RULE:[1:$1@$0](.*@REALM2.COM)s/@.*//");
        catalogRules.put("catalog3", "RULE:[1:$1@$0](.*@REALM3.COM)s/@.*//");

        // 设置多个catalog的规则
        for (Map.Entry<String, String> entry : catalogRules.entrySet()) {
            catalogConfig.addCatalogAuthToLocalRule(entry.getKey(), entry.getValue());
        }

        // 并发访问验证
        catalogRules.entrySet().parallelStream().forEach(entry -> {
            String catalogName = entry.getKey();
            String expectedRule = entry.getValue();
            
            Configuration config = authentication.getHadoopConfiguration(catalogName);
            assertEquals(config.get("hadoop.security.auth_to_local"), expectedRule);
        });
    }

    @Test
    public void testFallbackToDefaultConfiguration()
    {
        String catalogName = "unknown_catalog";
        
        // 对于未配置的catalog，应该返回默认配置
        Configuration config = authentication.getHadoopConfiguration(catalogName);
        assertNotNull(config);
        
        // 验证使用了默认的Hadoop配置
        assertTrue(config.get("hadoop.security.authentication") != null);
    }

    @Test
    public void testCatalogRemoval()
    {
        String catalogName = "temp_catalog";
        String authToLocalRule = "RULE:[1:$1@$0](.*@TEMP.COM)s/@.*//";

        // 添加catalog配置
        catalogConfig.addCatalogAuthToLocalRule(catalogName, authToLocalRule);
        Configuration config = authentication.getHadoopConfiguration(catalogName);
        assertEquals(config.get("hadoop.security.auth_to_local"), authToLocalRule);

        // 移除catalog配置
        catalogConfig.removeCatalogAuthToLocalRule(catalogName);
        Configuration configAfterRemoval = authentication.getHadoopConfiguration(catalogName);
        
        // 验证移除后使用默认配置
        assertNotNull(configAfterRemoval);
        assertTrue(configAfterRemoval.get("hadoop.security.auth_to_local") == null ||
                  !configAfterRemoval.get("hadoop.security.auth_to_local").equals(authToLocalRule));
    }
}