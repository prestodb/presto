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
package com.facebook.presto.hive.aws.lakeformation;

import com.facebook.presto.hive.metastore.MetastoreContext;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LakeFormationSecurityMapping
{
    private final Predicate<String> user;
    private final String iamRole;

    @JsonCreator
    public LakeFormationSecurityMapping(
            @JsonProperty("user") Optional<Pattern> user,
            @JsonProperty("iamRole") Optional<String> iamRole)
    {
        this.user = requireNonNull(user, "user is null")
                .map(LakeFormationSecurityMapping::toPredicate)
                .orElse(x -> true);
        this.iamRole = requireNonNull(iamRole, "iamRole is null").orElse(null);

        checkArgument(iamRole.isPresent(), "IAM Role must be provided");
    }

    public boolean matches(MetastoreContext metastoreContext)
    {
        return user.test(metastoreContext.getUsername());
    }

    public String getIamRole()
    {
        return iamRole;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("user", user)
                .add("iamRole", iamRole)
                .toString();
    }

    private static Predicate<String> toPredicate(Pattern pattern)
    {
        return value -> pattern.matcher(value).matches();
    }
}
