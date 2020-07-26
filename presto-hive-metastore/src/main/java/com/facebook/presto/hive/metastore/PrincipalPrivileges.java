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
package com.facebook.presto.hive.metastore;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import java.util.Set;

import static com.facebook.presto.spi.security.PrincipalType.ROLE;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PrincipalPrivileges
{
    private final SetMultimap<String, HivePrivilegeInfo> userPrivileges;
    private final SetMultimap<String, HivePrivilegeInfo> rolePrivileges;

    public PrincipalPrivileges(
            Multimap<String, HivePrivilegeInfo> userPrivileges,
            Multimap<String, HivePrivilegeInfo> rolePrivileges)
    {
        this.userPrivileges = ImmutableSetMultimap.copyOf(requireNonNull(userPrivileges, "userPrivileges is null"));
        this.rolePrivileges = ImmutableSetMultimap.copyOf(requireNonNull(rolePrivileges, "rolePrivileges is null"));
    }

    public static PrincipalPrivileges fromHivePrivilegeInfos(Set<HivePrivilegeInfo> hivePrivileges)
    {
        Multimap<String, HivePrivilegeInfo> userPrivileges = hivePrivileges
                .stream()
                .filter(privilege -> privilege.getGrantee().getType() == USER)
                .collect(toImmutableListMultimap(privilege -> privilege.getGrantee().getName(), identity()));

        Multimap<String, HivePrivilegeInfo> rolePrivileges = hivePrivileges
                .stream()
                .filter(privilege -> privilege.getGrantee().getType() == ROLE)
                .collect(toImmutableListMultimap(privilege -> privilege.getGrantee().getName(), identity()));
        return new PrincipalPrivileges(userPrivileges, rolePrivileges);
    }

    public SetMultimap<String, HivePrivilegeInfo> getUserPrivileges()
    {
        return userPrivileges;
    }

    public SetMultimap<String, HivePrivilegeInfo> getRolePrivileges()
    {
        return rolePrivileges;
    }
}
