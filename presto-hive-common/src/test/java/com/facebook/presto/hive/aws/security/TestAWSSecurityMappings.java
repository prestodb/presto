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
package com.facebook.presto.hive.aws.security;

import com.facebook.presto.spi.security.AccessDeniedException;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.plugin.base.JsonUtils.parseJson;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestAWSSecurityMappings
{
    private static final String DEFAULT_USER = "defaultUser";

    @Test
    public void testValidAWSLakeFormationMapping()
    {
        String lakeFormationSecurityMappingConfigPath =
                this.getClass().getClassLoader().getResource("com.facebook.presto.hive.aws.security/aws-security-mapping-lakeformation-valid.json").getPath();

        AWSSecurityMappings mappings = parseJson(new File(lakeFormationSecurityMappingConfigPath).toPath(), AWSSecurityMappings.class);

        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/admin_role").getIamRole(),
                mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().withUser("admin").getUser()).getIamRole().get());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/analyst_role").getIamRole(),
                mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().withUser("analyst").getUser()).getIamRole().get());
        assertEquals(MappingResult.role("arn:aws:iam::123456789101:role/default_role").getIamRole(),
                mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().getUser()).getIamRole().get());
    }

    @Test(
            expectedExceptions = VerifyException.class,
            expectedExceptionsMessageRegExp =
                    "(iamRole is mandatory for AWS Lake Formation Security Mapping|Basic AWS Credentials are not supported for AWS Lake Formation Security Mapping)")
    public void testInvalidAWSLakeFormationMapping()
    {
        String lakeFormationSecurityMappingConfigPath =
                this.getClass().getClassLoader().getResource("com.facebook.presto.hive.aws.security/aws-security-mapping-lakeformation-invalid.json").getPath();

        AWSSecurityMappings mappings = parseJson(new File(lakeFormationSecurityMappingConfigPath).toPath(), AWSSecurityMappings.class);

        // Fails with VerifyException: iamRole is mandatory for AWS Lake Formation Security Mapping
        mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().withUser("admin").getUser());

        // Fails with VerifyException: Basic AWS Credentials are not supported for AWS Lake Formation Security Mapping
        mappings.getAWSLakeFormationSecurityMapping(MappingSelector.empty().withUser("analyst").getUser());
    }

    @Test(
            expectedExceptions = VerifyException.class,
            expectedExceptionsMessageRegExp = "s3Prefix is not supported for AWS Lake Formation Security Mapping")
    public void testLakeFormationRejectsS3Prefix()
    {
        s3PrefixMappings("aws-security-mapping-lakeformation-with-s3prefix.json")
                .getAWSLakeFormationSecurityMapping("admin");
    }

    /**
     * Every criterion in an entry has to match, and the first entry that matches wins. The
     * identity-and-prefix entry is listed ahead of the bucket-wide one, so the same location
     * resolves differently depending on who is asking.
     */
    @Test
    public void testAllCriteriaMustMatchAndFirstMatchWins()
    {
        AWSSecurityMappings mappings = s3PrefixMappings();
        URI location = URI.create("s3a://bucket-a/sales/day=1/f.parquet");

        AWSSecurityMapping analyst = mappings.getAWSS3SecurityMapping("analyst", location);
        assertEquals(analyst.getCredentials().get().getAWSAccessKeyId(), "sales-access-key");
        assertEquals(analyst.getCredentials().get().getAWSSecretKey(), "sales-secret-key");
        assertFalse(analyst.getIamRole().isPresent());

        AWSSecurityMapping other = mappings.getAWSS3SecurityMapping("someone-else", location);
        assertEquals(other.getIamRole().get(), "arn:aws:iam::123456789101:role/bucket_a_role");
    }

    @Test
    public void testMappingWithoutPrefixActsAsCatchAll()
    {
        assertEquals(
                s3PrefixMappings().getAWSS3SecurityMapping(DEFAULT_USER, URI.create("s3a://unmapped-bucket/f.parquet"))
                        .getIamRole().get(),
                "arn:aws:iam::123456789101:role/default_role");
    }

    /**
     * Security mapping is an access-control feature, so a location no entry covers is denied
     * rather than silently falling back to the catalog-wide credentials.
     */
    @Test(expectedExceptions = AccessDeniedException.class)
    public void testUnmatchedLocationIsDeniedWhenNoCatchAllExists()
    {
        s3PrefixMappings("aws-security-mapping-s3-prefix-no-catch-all.json")
                .getAWSS3SecurityMapping(DEFAULT_USER, URI.create("s3a://bucket-z/f.parquet"));
    }

    /**
     * A single entry may list several prefixes, covering multiple buckets or one bucket addressed
     * through more than one scheme, without duplicating the credentials. All of them share one
     * cache scope, since they resolve to the same credentials.
     */
    @Test
    public void testMultiplePrefixesInOneEntry()
    {
        AWSSecurityMappings mappings = s3PrefixMappings();

        AWSSecurityMapping first = null;
        for (String location : new String[] {
                "s3a://shared-bucket/f.parquet",
                "s3://shared-bucket/f.parquet",
                "s3a://other-shared-bucket/nested/f.parquet"}) {
            AWSSecurityMapping mapping = mappings.getAWSS3SecurityMapping(DEFAULT_USER, URI.create(location));
            assertEquals(mapping.getCredentials().get().getAWSAccessKeyId(), "shared-access-key",
                    "unexpected mapping for " + location);
            if (first == null) {
                first = mapping;
            }
            assertEquals(mapping.getS3CacheScope(), first.getS3CacheScope(),
                    "unexpected cache scope for " + location);
        }
    }

    /**
     * Prefixes may also be given as a bare string rather than a list. A scheme no prefix lists is
     * not covered, so bucket-c, listed only under s3a://, falls through to the catch-all when
     * addressed through s3://.
     */
    @Test
    public void testSingleStringPrefixAndUnlistedSchemeAreNotCovered()
    {
        AWSSecurityMappings mappings = s3PrefixMappings();

        assertEquals(
                mappings.getAWSS3SecurityMapping(DEFAULT_USER, URI.create("s3a://bucket-c/f.parquet")).getIamRole().get(),
                "arn:aws:iam::123456789101:role/bucket_c_role");
        assertEquals(
                mappings.getAWSS3SecurityMapping(DEFAULT_USER, URI.create("s3://bucket-c/f.parquet")).getIamRole().get(),
                "arn:aws:iam::123456789101:role/default_role");
    }

    @Test
    public void testCatchAllEntryHasNoCacheScope()
    {
        assertFalse(
                s3PrefixMappings().getAWSS3SecurityMapping(DEFAULT_USER, URI.create("s3a://unmapped-bucket/f.parquet"))
                        .getS3CacheScope().isPresent());
    }

    /**
     * Prefix matching happens on path-segment boundaries. Enumerated rather than spot-checked,
     * because the same property has to hold at the bucket level and at any depth of key, for the
     * prefix itself as well as for what lies beneath it, and with or without a trailing slash.
     */
    @Test
    public void testPrefixMatchesOnPathSegmentBoundaries()
    {
        Object[][] cases = {
                // prefix, location, expected
                // the prefix itself, which is how a directory arrives once Path strips the slash
                {"s3a://bucket/sales", "s3a://bucket/sales", true},
                {"s3a://bucket/sales/", "s3a://bucket/sales", true},
                {"s3a://bucket", "s3a://bucket", true},
                {"s3a://bucket/", "s3a://bucket", true},
                // Path keeps the slash for a bucket root, since the root path is itself "/"
                {"s3a://bucket", "s3a://bucket/", true},
                {"s3a://bucket/", "s3a://bucket/", true},
                // anything beneath it
                {"s3a://bucket/sales", "s3a://bucket/sales/day=1/f.parquet", true},
                {"s3a://bucket/sales/", "s3a://bucket/sales/day=1/f.parquet", true},
                {"s3a://bucket", "s3a://bucket/sales/f.parquet", true},
                {"s3a://bucket/", "s3a://bucket/sales/f.parquet", true},
                // a sibling sharing leading characters, at the key level
                {"s3a://bucket/sales", "s3a://bucket/sales-archive/f.parquet", false},
                {"s3a://bucket/sales/", "s3a://bucket/sales-archive/f.parquet", false},
                {"s3a://bucket/sales", "s3a://bucket/salesX", false},
                // a sibling sharing leading characters, at the bucket level
                {"s3a://prod", "s3a://prod-secrets/creds.parquet", false},
                {"s3a://prod/", "s3a://prod-secrets/creds.parquet", false},
                // a partial key segment is not a boundary
                {"s3a://bucket/data-2024", "s3a://bucket/data-2024-01/f.parquet", false},
                // an unrelated location
                {"s3a://bucket/sales", "s3a://other/sales/f.parquet", false},
        };

        for (Object[] testCase : cases) {
            String prefix = (String) testCase[0];
            String location = (String) testCase[1];
            boolean expected = (boolean) testCase[2];

            assertEquals(
                    mappingWithPrefixes(prefix).matchesS3(DEFAULT_USER, location),
                    expected,
                    format("prefix %s against location %s", prefix, location));
        }
    }

    /**
     * Writing a prefix with or without a trailing slash makes no difference, so the two forms share
     * a cache scope rather than splitting one credential scope across two filesystem cache slots.
     */
    @Test
    public void testTrailingSlashDoesNotAffectCacheScope()
    {
        assertEquals(
                mappingWithPrefixes("s3a://bucket/sales/").getS3CacheScope(),
                mappingWithPrefixes("s3a://bucket/sales").getS3CacheScope());
    }

    private static AWSSecurityMapping mappingWithPrefixes(String... prefixes)
    {
        return new AWSSecurityMapping(
                Optional.empty(),
                ImmutableList.copyOf(prefixes),
                Optional.of("arn:aws:iam::123456789101:role/some_role"),
                Optional.empty(),
                Optional.empty());
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "s3Prefix must include a bucket.*")
    public void testPrefixWithoutBucketIsRejected()
    {
        mappingWithPrefixes("s3a:///");
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "s3Prefix must include a scheme.*")
    public void testPrefixWithoutSchemeIsRejected()
    {
        mappingWithPrefixes("just-a-bucket/path");
    }

    private AWSSecurityMappings s3PrefixMappings()
    {
        return s3PrefixMappings("aws-security-mapping-s3-prefix-valid.json");
    }

    private AWSSecurityMappings s3PrefixMappings(String resourceName)
    {
        String path = this.getClass().getClassLoader()
                .getResource("com.facebook.presto.hive.aws.security/" + resourceName).getPath();
        return parseJson(new File(path).toPath(), AWSSecurityMappings.class);
    }

    private static class MappingSelector
    {
        private static MappingSelector empty()
        {
            return new MappingSelector(DEFAULT_USER);
        }

        private final String user;

        private MappingSelector(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        private MappingSelector withUser(String user)
        {
            return new MappingSelector(user);
        }

        private String getUser()
        {
            return user;
        }
    }

    private static class MappingResult
    {
        private static MappingResult role(String role)
        {
            return new MappingResult(role);
        }

        private final String iamRole;

        private MappingResult(String iamRole)
        {
            this.iamRole = requireNonNull(iamRole, "role is null");
        }

        private String getIamRole()
        {
            return iamRole;
        }
    }
}
