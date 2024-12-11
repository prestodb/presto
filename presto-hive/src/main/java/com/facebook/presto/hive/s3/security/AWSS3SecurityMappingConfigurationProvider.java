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

import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.aws.security.AWSSecurityMapping;
import com.facebook.presto.hive.aws.security.AWSSecurityMappings;
import com.facebook.presto.hive.aws.security.AWSSecurityMappingsSupplier;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACCESS_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_IAM_ROLE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SECRET_KEY;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AWSS3SecurityMappingConfigurationProvider
        implements DynamicConfigurationProvider
{
    private static final Set<String> SCHEMES = ImmutableSet.of("s3", "s3a", "s3n");

    private final Supplier<AWSSecurityMappings> mappings;

    @Inject
    public AWSS3SecurityMappingConfigurationProvider(
            @ForAWSS3DynamicConfigurationProvider AWSSecurityMappingsSupplier awsSecurityMappingsSupplier)
    {
        this(requireNonNull(awsSecurityMappingsSupplier, "awsSecurityMappingsSupplier is null").getMappingsSupplier());
    }

    private AWSS3SecurityMappingConfigurationProvider(Supplier<AWSSecurityMappings> mappings)
    {
        this.mappings = requireNonNull(mappings, "mappings is null");
    }

    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        if (!SCHEMES.contains(uri.getScheme())) {
            return;
        }

        AWSSecurityMapping awsS3SecurityMapping = mappings.get().getAWSS3SecurityMapping(context.getIdentity().getUser());

        checkArgument(
                awsS3SecurityMapping.getIamRole().isPresent() || awsS3SecurityMapping.getCredentials().isPresent(),
                "AWS S3 security mapping must have role or credentials");

        awsS3SecurityMapping.getCredentials().ifPresent(credentials -> {
            configuration.set(S3_ACCESS_KEY, credentials.getAWSAccessKeyId());
            configuration.set(S3_SECRET_KEY, credentials.getAWSSecretKey());
        });

        awsS3SecurityMapping.getIamRole().ifPresent(role -> {
            configuration.set(S3_IAM_ROLE, role);
        });
    }
}
