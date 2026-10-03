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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class AWSSecurityMappings
{
    private final List<AWSSecurityMapping> awsSecurityMappings;

    @JsonCreator
    public AWSSecurityMappings(@JsonProperty("mappings") List<AWSSecurityMapping> awsSecurityMappings)
    {
        checkArgument(awsSecurityMappings != null, "No AWS Security mappings configured");

        this.awsSecurityMappings = ImmutableList.copyOf(awsSecurityMappings);
    }

    public AWSSecurityMapping getAWSLakeFormationSecurityMapping(String user)
    {
        Optional<AWSSecurityMapping> awsSecurityMapping = awsSecurityMappings.stream()
                .filter(mapping -> (mapping.matches(user)))
                .findFirst();

        if (!awsSecurityMapping.isPresent()) {
            throw new AccessDeniedException("No matching AWS Lake Formation Security Mapping");
        }

        verify(!awsSecurityMapping.get().getCredentials().isPresent(),
                "Basic AWS Credentials are not supported for AWS Lake Formation Security Mapping");

        verify(awsSecurityMapping.get().getIamRole().isPresent(),
                "iamRole is mandatory for AWS Lake Formation Security Mapping");

        verify(awsSecurityMapping.get().getS3Prefixes().isEmpty(),
                "s3Prefix is not supported for AWS Lake Formation Security Mapping");

        return awsSecurityMapping.get();
    }

    /**
     * Returns the first mapping matching both the identity and the location, so more specific
     * {@code s3Prefix} entries must be listed ahead of broader ones. A mapping with no
     * {@code s3Prefix} matches every location and therefore acts as a catch-all; an identity-only
     * mapping file keeps its previous behaviour.
     */
    public AWSSecurityMapping getAWSS3SecurityMapping(String user, URI uri)
    {
        requireNonNull(uri, "uri is null");

        Optional<AWSSecurityMapping> awsSecurityMapping = awsSecurityMappings.stream()
                .filter(mapping -> mapping.matchesS3(user, uri.toString()))
                .findFirst();

        return awsSecurityMapping.orElseThrow(() -> new AccessDeniedException("No matching AWS S3 Security Mapping"));
    }
}
