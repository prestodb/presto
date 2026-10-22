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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACCESS_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_IAM_ROLE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SECRET_KEY;
import static com.google.common.io.Resources.getResource;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.PrestoFileSystemCache.PRESTO_CACHE_KEY_QUALIFIER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class TestAWSS3SecurityMapping
{
    private static final String DEFAULT_USER = "defaultUser";
    private static final String PREFIX_MAPPING_FILE = "aws-security-mapping-s3-prefix.json";

    @Test
    public void testAWSS3SecurityMapping()
    {
        DynamicConfigurationProvider provider = createProvider("aws-security-mapping-with-default-role.json");

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
            expectedExceptionsMessageRegExp =
                    "Access Denied: No matching AWS S3 Security Mapping for user 'defaultUser' and location 's3://defaultBucket/'")
    public void testFailAWSS3SecurityMapping()
    {
        DynamicConfigurationProvider provider = createProvider("aws-security-mapping-without-default-role.json");

        // matches no security mapping -- access denied
        Configuration configuration = new Configuration(false);
        applyMapping(provider, MappingSelector.empty().withUser("defaultUser"), configuration);
    }

    /**
     * Both prefixes live in one bucket, so the filesystem cache key, which carries no path, cannot
     * tell them apart. Distinct qualifiers are what give them separate cache slots.
     */
    @Test
    public void testPrefixesInSameBucketGetDistinctQualifiers()
    {
        DynamicConfigurationProvider provider = createProvider(PREFIX_MAPPING_FILE);

        Configuration sales = resolve(provider, "s3a://bucket-a/sales/day=1/f.parquet");
        Configuration hr = resolve(provider, "s3a://bucket-a/hr/day=1/f.parquet");

        assertEquals(sales.get(S3_ACCESS_KEY), "sales-access-key");
        assertEquals(sales.get(S3_SECRET_KEY), "sales-secret-key");
        assertEquals(hr.get(S3_ACCESS_KEY), "hr-access-key");

        assertNotNull(sales.get(PRESTO_CACHE_KEY_QUALIFIER));
        assertNotNull(hr.get(PRESTO_CACHE_KEY_QUALIFIER));
        assertNotEquals(sales.get(PRESTO_CACHE_KEY_QUALIFIER), hr.get(PRESTO_CACHE_KEY_QUALIFIER));
    }

    /**
     * A mapping without a prefix is no finer than the bucket already in the cache key, so it needs
     * no qualifier and must not set one.
     */
    @Test
    public void testCatchAllMappingSetsNoQualifier()
    {
        Configuration configuration = resolve(createProvider(PREFIX_MAPPING_FILE), "s3a://unmapped-bucket/f.parquet");

        assertEquals(configuration.get(S3_IAM_ROLE), "arn:aws:iam::123456789101:role/default_role");
        assertNull(configuration.get(PRESTO_CACHE_KEY_QUALIFIER));
    }

    @Test
    public void testNonS3SchemeIsUntouched()
    {
        Configuration configuration = resolve(createProvider(PREFIX_MAPPING_FILE), "hdfs://bucket-a/sales/f.parquet");

        assertNull(configuration.get(S3_ACCESS_KEY));
        assertNull(configuration.get(PRESTO_CACHE_KEY_QUALIFIER));
    }

    /**
     * The assertions above only show that the provider emits different qualifier strings. This
     * drives the filesystem cache with those same configurations to show the strings are acted on:
     * two prefixes in one bucket get separate cached filesystems, and one prefix reuses its own.
     */
    @Test
    public void testQualifierSeparatesCachedFileSystems()
            throws IOException
    {
        DynamicConfigurationProvider provider = createProvider(PREFIX_MAPPING_FILE);

        FileSystem sales = fileSystem(resolve(provider, "s3a://bucket-a/sales/day=1/f.parquet"));
        FileSystem salesAgain = fileSystem(resolve(provider, "s3a://bucket-a/sales/day=2/other.parquet"));
        FileSystem hr = fileSystem(resolve(provider, "s3a://bucket-a/hr/day=1/f.parquet"));

        try {
            assertSame(sales, salesAgain);
            assertNotSame(sales, hr);
        }
        finally {
            sales.close();
            hr.close();
        }
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
        applyMapping(provider, selector, new Path("s3://defaultBucket/").toUri(), configuration);
    }

    private static void applyMapping(DynamicConfigurationProvider provider, MappingSelector selector, URI uri, Configuration configuration)
    {
        provider.updateConfiguration(configuration, selector.getHdfsContext(), uri);
    }

    private static DynamicConfigurationProvider createProvider(String resourceName)
    {
        AWSSecurityMappingConfig mappingConfig = new AWSSecurityMappingConfig()
                .setMappingType(AWSSecurityMappingType.S3)
                .setConfigFile(new File(getResource(TestAWSS3SecurityMapping.class, resourceName).getPath()));

        return new AWSS3SecurityMappingConfigurationProvider(
                new AWSSecurityMappingsSupplier(mappingConfig.getConfigFile(), mappingConfig.getRefreshPeriod()));
    }

    private static Configuration resolve(DynamicConfigurationProvider provider, String location)
    {
        Configuration configuration = new Configuration(false);
        applyMapping(provider, MappingSelector.empty(), URI.create(location), configuration);
        return configuration;
    }

    private static FileSystem fileSystem(Configuration configuration)
            throws IOException
    {
        return HDFS_ENVIRONMENT.getFileSystem(DEFAULT_USER, new Path("/"), configuration);
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
