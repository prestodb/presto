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
package com.facebook.presto.hive.aws.security.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AWSS3SecurityMapping
{
    private final Predicate<String> user;
    private final Optional<String> iamRole;
    private final Optional<BasicAWSCredentials> credentials;

    @JsonCreator
    public AWSS3SecurityMapping(
            @JsonProperty("user") Optional<Pattern> user,
            @JsonProperty("iamRole") Optional<String> iamRole,
            @JsonProperty("accessKey") Optional<String> accessKey,
            @JsonProperty("secretKey") Optional<String> secretKey)
    {
        this.user = requireNonNull(user, "user is null")
                .map(AWSS3SecurityMapping::toPredicate)
                .orElse(x -> true);

        this.iamRole = requireNonNull(iamRole, "iamRole is null");

        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(secretKey, "secretKey is null");
        checkArgument(accessKey.isPresent() == secretKey.isPresent(), "accessKey and secretKey must be provided together");
        this.credentials = accessKey.map(access -> new BasicAWSCredentials(access, secretKey.get()));

        checkArgument(iamRole.isPresent() || credentials.isPresent(), "must provide role and/or credentials");
    }

    public boolean matches(ConnectorIdentity identity)
    {
        return user.test(identity.getUser());
    }

    public Optional<String> getIamRole()
    {
        return iamRole;
    }

    public Optional<BasicAWSCredentials> getCredentials()
    {
        return credentials;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("user", user)
                .add("iamRole", iamRole)
                .add("credentials", credentials)
                .toString();
    }

    private static Predicate<String> toPredicate(Pattern pattern)
    {
        return value -> pattern.matcher(value).matches();
    }
}
