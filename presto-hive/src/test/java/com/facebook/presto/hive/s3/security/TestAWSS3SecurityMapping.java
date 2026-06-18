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
package com.facebook.presto.hive.s3.security;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.hive.aws.security.AWSSecurityMappingConfig;
import com.facebook.presto.hive.aws.security.AWSSecurityMappingType;
import com.facebook.presto.hive.aws.security.AWSSecurityMappingsSupplier;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACCESS_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_IAM_ROLE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SECRET_KEY;
import static com.google.common.io.Resources.getResource;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestAWSS3SecurityMapping
{
    private static final String DEFAULT_USER = "defaultUser";

    @Test
    public void testAWSS3SecurityMapping()
    {
        AWSSecurityMappingConfig mappingConfig = new AWSSecurityMappingConfig()
                .setMappingType(AWSSecurityMappingType.S3)
                .setConfigFile(new File(getResource(getClass(), "aws-security-mapping-with-default-role.json").getPath()));

        AWSSecurityMappingsSupplier awsSecurityMappingsSupplier =
                new AWSSecurityMappingsSupplier(mappingConfig.getConfigFile(), mappingConfig.getRefreshPeriod());

        DynamicConfigurationProvider provider = new AWSS3SecurityMappingConfigurationProvider(awsSecurityMappingsSupplier);

        // matches user -- mapping provides credentials
        assertMapping(
                provider,
                MappingSelector.empty().withUser("admin"),
                MappingResult.credentials("AKIAxxxaccess", "iXbXxxxsecret"));

        // matches user regex -- mapping provides iam role
        assertMapping(
                provider,
                MappingSelector.empty().withUser("analyst"),
                MappingResult.role("arn:aws:iam::123456789101:role/analyst_and_scientist_role"));

        // matches empty rule at end -- default role used
        assertMapping(
                provider,
                MappingSelector.empty().withUser("defaultUser"),
                MappingResult.role("arn:aws:iam::123456789101:role/default"));
    }

    @Test(
            expectedExceptions = AccessDeniedException.class,
            expectedExceptionsMessageRegExp = "Access Denied: No matching AWS S3 Security Mapping")
    public void testFailAWSS3SecurityMapping()
    {
        AWSSecurityMappingConfig mappingConfig = new AWSSecurityMappingConfig()
                .setMappingType(AWSSecurityMappingType.S3)
                .setConfigFile(new File(getResource(getClass(), "aws-security-mapping-without-default-role.json").getPath()));

        AWSSecurityMappingsSupplier awsSecurityMappingsSupplier =
                new AWSSecurityMappingsSupplier(mappingConfig.getConfigFile(), mappingConfig.getRefreshPeriod());

        DynamicConfigurationProvider provider = new AWSS3SecurityMappingConfigurationProvider(awsSecurityMappingsSupplier);

        // matches no security mapping -- access denied
        Configuration configuration = new Configuration(false);
        applyMapping(provider, MappingSelector.empty().withUser("defaultUser"), configuration);
    }

    private static void assertMapping(DynamicConfigurationProvider provider, MappingSelector selector, MappingResult mappingResult)
    {
        Configuration configuration = new Configuration(false);

        assertNull(configuration.get(S3_ACCESS_KEY));
        assertNull(configuration.get(S3_SECRET_KEY));
        assertNull(configuration.get(S3_IAM_ROLE));

        applyMapping(provider, selector, configuration);

        if (mappingResult.getAccessKey().isPresent()) {
            assertEquals(configuration.get(S3_ACCESS_KEY), mappingResult.getAccessKey().get());
            assertEquals(configuration.get(S3_SECRET_KEY), mappingResult.getSecretKey().get());
        }
        else {
            assertEquals(configuration.get(S3_IAM_ROLE), mappingResult.getRole().get());
        }
    }

    private static void applyMapping(DynamicConfigurationProvider provider, MappingSelector selector, Configuration configuration)
    {
        provider.updateConfiguration(configuration, selector.getHdfsContext(), new Path("s3://defaultBucket/").toUri());
    }

    private static class MappingSelector
    {
        private final String user;

        private MappingSelector(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        private static MappingSelector empty()
        {
            return new MappingSelector(DEFAULT_USER);
        }

        private MappingSelector withUser(String user)
        {
            return new MappingSelector(user);
        }

        private HdfsContext getHdfsContext()
        {
            ConnectorSession connectorSession = new TestingConnectorSession(
                    new ConnectorIdentity(
                            user, Optional.empty(), Optional.empty()),
                    new HiveSessionProperties(
                            new HiveClientConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig(), new CacheConfig()
                    ).getSessionProperties());
            return new HdfsContext(connectorSession, "schema");
        }
    }

    private static class MappingResult
    {
        private static MappingResult credentials(String accessKey, String secretKey)
        {
            return new MappingResult(Optional.of(accessKey), Optional.of(secretKey), Optional.empty());
        }

        private static MappingResult role(String role)
        {
            return new MappingResult(Optional.empty(), Optional.empty(), Optional.of(role));
        }

        private final Optional<String> accessKey;
        private final Optional<String> secretKey;
        private final Optional<String> role;

        private MappingResult(Optional<String> accessKey, Optional<String> secretKey, Optional<String> role)
        {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.role = role;
        }

        private Optional<String> getAccessKey()
        {
            return accessKey;
        }

        private Optional<String> getSecretKey()
        {
            return secretKey;
        }

        private Optional<String> getRole()
        {
            return role;
        }
    }
}
