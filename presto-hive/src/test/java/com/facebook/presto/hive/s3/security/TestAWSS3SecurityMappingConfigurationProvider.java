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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.aws.security.AWSSecurityMappingsSupplier;
import com.facebook.presto.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.PRESTO_CACHE_KEY_QUALIFIER;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACCESS_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_IAM_ROLE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SECRET_KEY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class TestAWSS3SecurityMappingConfigurationProvider
{
    private static final String USER = "test-user";

    /**
     * Both prefixes live in one bucket, so the filesystem cache key, which carries no path, cannot
     * tell them apart. Distinct qualifiers are what give them separate cache slots. Also covers
     * prefix-scoped credentials reaching the configuration at all, since the two entries resolve to
     * different keys.
     */
    @Test
    public void testPrefixesInSameBucketGetDistinctQualifiers()
    {
        Configuration sales = resolve(USER, "s3a://bucket-a/sales/day=1/f.parquet");
        Configuration hr = resolve(USER, "s3a://bucket-a/hr/day=1/f.parquet");

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
        Configuration conf = resolve(USER, "s3a://unmapped-bucket/f.parquet");

        assertEquals(conf.get(S3_IAM_ROLE), "arn:aws:iam::123456789101:role/default_role");
        assertNull(conf.get(PRESTO_CACHE_KEY_QUALIFIER));
    }

    @Test
    public void testNonS3SchemeIsUntouched()
    {
        Configuration conf = new Configuration(false);
        provider().updateConfiguration(conf, context(USER), URI.create("hdfs://bucket-a/sales/f.parquet"));

        assertNull(conf.get(S3_ACCESS_KEY));
        assertNull(conf.get(PRESTO_CACHE_KEY_QUALIFIER));
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
        FileSystem sales = fileSystem(resolve(USER, "s3a://bucket-a/sales/day=1/f.parquet"));
        FileSystem salesAgain = fileSystem(resolve(USER, "s3a://bucket-a/sales/day=2/other.parquet"));
        FileSystem hr = fileSystem(resolve(USER, "s3a://bucket-a/hr/day=1/f.parquet"));

        assertSame(sales, salesAgain);
        assertNotSame(sales, hr);
    }

    private static FileSystem fileSystem(Configuration configuration)
            throws IOException
    {
        return HDFS_ENVIRONMENT.getFileSystem(USER, new Path("/"), configuration);
    }

    private static Configuration resolve(String user, String location)
    {
        Configuration conf = new Configuration(false);
        provider().updateConfiguration(conf, context(user), URI.create(location));
        return conf;
    }

    private static AWSS3SecurityMappingConfigurationProvider provider()
    {
        String path = TestAWSS3SecurityMappingConfigurationProvider.class.getClassLoader()
                .getResource("com.facebook.presto.hive.s3.security/aws-s3-security-mapping-prefix.json").getPath();
        return new AWSS3SecurityMappingConfigurationProvider(
                new AWSSecurityMappingsSupplier(Optional.of(new File(path)), new Duration(1, TimeUnit.MINUTES)));
    }

    private static HdfsContext context(String user)
    {
        return new HdfsContext(Optional.empty(),
                new ConnectorIdentity(user, Optional.empty(), Optional.empty()),
                Optional.empty(),
                Optional.of("test_query_id"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
