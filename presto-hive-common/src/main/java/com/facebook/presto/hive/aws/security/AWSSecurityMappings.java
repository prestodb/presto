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

import com.facebook.presto.hive.aws.security.lakeformation.AWSLakeFormationSecurityMapping;
import com.facebook.presto.hive.aws.security.s3.AWSS3SecurityMapping;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class AWSSecurityMappings
{
    private final List<AWSLakeFormationSecurityMapping> awsLakeFormationSecurityMappings;
    private final List<AWSS3SecurityMapping> awsS3SecurityMappings;

    @JsonCreator
    public AWSSecurityMappings(
            @JsonProperty("lakeformation") List<AWSLakeFormationSecurityMapping> awsLakeFormationSecurityMappings,
            @JsonProperty("s3") List<AWSS3SecurityMapping> awsS3SecurityMappings)
    {
        checkArgument(
                (awsLakeFormationSecurityMappings != null) || (awsS3SecurityMappings != null),
                "No AWS Security mappings configured");
        checkArgument(
                (awsLakeFormationSecurityMappings == null) || (awsS3SecurityMappings == null),
                "AWS S3 Security Mapping is not allowed if AWS Lake Formation Security Mapping is configured");
        this.awsLakeFormationSecurityMappings = awsLakeFormationSecurityMappings != null ?
                ImmutableList.copyOf(awsLakeFormationSecurityMappings) :
                ImmutableList.of();
        this.awsS3SecurityMappings = awsS3SecurityMappings != null ?
                ImmutableList.copyOf(awsS3SecurityMappings) :
                ImmutableList.of();
    }

    public boolean isAWSLakeFormationSecurityMappingEnabled()
    {
        return !awsLakeFormationSecurityMappings.isEmpty();
    }

    public boolean isAWSS3SecurityMappingEnabled()
    {
        return !awsS3SecurityMappings.isEmpty();
    }

    public Optional<AWSLakeFormationSecurityMapping> getAWSLakeFormationSecurityMapping(String user)
    {
        return awsLakeFormationSecurityMappings.stream()
                .filter(mapping -> (mapping.matches(user) && !mapping.getIamRole().isEmpty()))
                .findFirst();
    }

    public Optional<AWSS3SecurityMapping> getAWSS3SecurityMapping(ConnectorIdentity identity)
    {
        return awsS3SecurityMappings.stream()
                .filter(mapping -> mapping.matches(identity))
                .findFirst();
    }
}
