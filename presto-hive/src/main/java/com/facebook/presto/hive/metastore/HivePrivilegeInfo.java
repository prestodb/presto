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

import com.facebook.presto.spi.security.Privilege;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
import static java.util.Locale.ENGLISH;

public class HivePrivilegeInfo
{
    public enum HivePrivilege
    {
        SELECT, INSERT, UPDATE, DELETE, OWNERSHIP;
    }

    private final HivePrivilege hivePrivilege;
    private final boolean grantOption;

    public HivePrivilegeInfo(HivePrivilege hivePrivilege, boolean grantOption)
    {
        this.hivePrivilege = hivePrivilege;
        this.grantOption = grantOption;
    }

    public HivePrivilege getHivePrivilege()
    {
        return hivePrivilege;
    }

    public boolean isGrantOption()
    {
        return grantOption;
    }

    public HivePrivilegeInfo withGrantOption(boolean grantOption)
    {
        return new HivePrivilegeInfo(hivePrivilege, grantOption);
    }

    public static Set<HivePrivilegeInfo> parsePrivilege(PrivilegeGrantInfo userGrant)
    {
        boolean withGrantOption = userGrant.isGrantOption();
        String name = userGrant.getPrivilege().toUpperCase(ENGLISH);
        switch (name) {
            case "ALL":
                return Arrays.asList(HivePrivilege.values()).stream()
                        .map(hivePrivilege -> new HivePrivilegeInfo(hivePrivilege, withGrantOption))
                        .collect(Collectors.toSet());
            case "SELECT":
                return ImmutableSet.of(new HivePrivilegeInfo(SELECT, withGrantOption));
            case "INSERT":
                return ImmutableSet.of(new HivePrivilegeInfo(INSERT, withGrantOption));
            case "UPDATE":
                return ImmutableSet.of(new HivePrivilegeInfo(UPDATE, withGrantOption));
            case "DELETE":
                return ImmutableSet.of(new HivePrivilegeInfo(DELETE, withGrantOption));
            case "OWNERSHIP":
                return ImmutableSet.of(new HivePrivilegeInfo(OWNERSHIP, withGrantOption));
        }
        return ImmutableSet.of();
    }

    public static HivePrivilege toHivePrivilege(Privilege privilege)
    {
        switch (privilege) {
            case SELECT:
                return SELECT;
            case INSERT:
                return INSERT;
            case DELETE:
                return DELETE;
        }
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hivePrivilege, grantOption);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HivePrivilegeInfo hivePrivilegeInfo = (HivePrivilegeInfo) o;
        return Objects.equals(hivePrivilege, hivePrivilegeInfo.hivePrivilege) &&
                Objects.equals(grantOption, hivePrivilegeInfo.grantOption);
    }
}
