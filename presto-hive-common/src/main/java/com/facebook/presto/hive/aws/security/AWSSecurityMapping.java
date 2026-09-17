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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.annotation.JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AWSSecurityMapping
{
    private static final String CACHE_SCOPE_SEPARATOR = "\u0000";
    private final Predicate<String> user;
    private final List<String> s3Prefixes;
    private final Optional<String> iamRole;
    private final Optional<BasicAWSCredentials> credentials;

    @JsonCreator
    public AWSSecurityMapping(
            @JsonProperty("user") Optional<Pattern> user,
            @JsonProperty("s3Prefix") @JsonFormat(with = ACCEPT_SINGLE_VALUE_AS_ARRAY) List<String> s3Prefix,
            @JsonProperty("iamRole") Optional<String> iamRole,
            @JsonProperty("accessKey") Optional<String> accessKey,
            @JsonProperty("secretKey") Optional<String> secretKey)
    {
        this.user = requireNonNull(user, "user is null")
                .map(AWSSecurityMapping::toPredicate)
                .orElse(x -> true);

        // Absent and empty both mean "any location", so no Optional is needed to tell them apart.
        this.s3Prefixes = s3Prefix == null
                ? ImmutableList.of()
                : s3Prefix.stream().map(AWSSecurityMapping::canonicalizeS3Prefix).collect(toImmutableList());
        this.iamRole = requireNonNull(iamRole, "iamRole is null");

        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(secretKey, "secretKey is null");
        checkArgument(accessKey.isPresent() == secretKey.isPresent(), "accessKey and secretKey must be provided together");
        this.credentials = accessKey.map(access -> new BasicAWSCredentials(access, secretKey.get()));
    }

    public boolean matches(String user)
    {
        return this.user.test(user);
    }

    /**
     * Matches when the identity matches and the location falls under any configured prefix. A
     * mapping with no prefix matches any location, which is what keeps identity-only mapping files
     * behaving exactly as before.
     * <p>
     * A prefix must be written with the same scheme as the locations it is meant to cover:
     * {@code s3a://bucket/} does not match an {@code s3://bucket/} location.
     */
    public boolean matchesS3(String user, String location)
    {
        return matches(user) && (s3Prefixes.isEmpty() || s3Prefixes.stream().anyMatch(prefix -> coversLocation(prefix, location)));
    }

    /**
     * Compares on path-segment boundaries rather than as a bare string prefix, matching the prefix
     * itself as well as anything beneath it.
     * <p>
     * A bare {@code startsWith} would be wrong in both directions. It lets a prefix spill onto a
     * sibling that merely shares its leading characters, so {@code s3a://bucket/sales} would lend
     * its credentials to {@code s3a://bucket/sales-archive/f.parquet}. And because
     * {@code org.apache.hadoop.fs.Path} strips trailing slashes, a directory arrives here as
     * {@code s3a://bucket/sales}, which a prefix written the natural way as
     * {@code s3a://bucket/sales/} would fail to match even though every file under it matches.
     */
    private static boolean coversLocation(String prefix, String location)
    {
        return location.equals(prefix) || location.startsWith(prefix + "/");
    }

    public List<String> getS3Prefixes()
    {
        return s3Prefixes;
    }

    /**
     * Identifies this entry's location scope for the filesystem cache qualifier, or empty when the
     * entry is not scoped to any prefix and so is no narrower than the bucket that the cache key
     * already covers.
     * <p>
     * Derived from the whole prefix list rather than the prefix that matched, because every
     * location this entry covers resolves to the same credentials and can therefore share one
     * cached filesystem. The identity is deliberately left out: the cache key carries
     * {@code realUser} already, so including it here would split a single credential scope across
     * users for no benefit.
     */
    public Optional<String> getS3CacheScope()
    {
        if (s3Prefixes.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.join(CACHE_SCOPE_SEPARATOR, s3Prefixes));
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
                .add("s3Prefixes", s3Prefixes)
                .add("iamRole", iamRole)
                .add("credentials", credentials)
                .toString();
    }

    private static Predicate<String> toPredicate(Pattern pattern)
    {
        return value -> pattern.matcher(value).matches();
    }

    /**
     * Validates a prefix and reduces it to the form {@link #coversLocation} compares against, which
     * carries no trailing slash. This is the same shape {@code org.apache.hadoop.fs.Path} produces
     * for the locations being matched, so {@code s3a://bucket/sales} and {@code s3a://bucket/sales/}
     * are equivalent ways of writing one prefix.
     */
    private static String canonicalizeS3Prefix(String prefix)
    {
        requireNonNull(prefix, "s3Prefix element is null");
        checkArgument(!prefix.isEmpty(), "s3Prefix element is empty");

        URI uri;
        try {
            uri = new URI(prefix);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException("s3Prefix is not a valid URI: " + prefix, e);
        }

        checkArgument(uri.getScheme() != null, "s3Prefix must include a scheme, such as s3a://bucket/: %s", prefix);
        checkArgument(uri.getAuthority() != null, "s3Prefix must include a bucket, such as s3a://bucket/: %s", prefix);

        String canonical = prefix;
        while (canonical.endsWith("/")) {
            canonical = canonical.substring(0, canonical.length() - 1);
        }
        return canonical;
    }
}
