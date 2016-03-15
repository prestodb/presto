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

import java.util.Set;

import static java.util.Locale.ENGLISH;

public enum HivePrivilege
{
    SELECT, INSERT, UPDATE, DELETE, OWNERSHIP, GRANT;

    public static Set<HivePrivilege> parsePrivilege(PrivilegeGrantInfo userGrant)
    {
        String name = userGrant.getPrivilege().toUpperCase(ENGLISH);
        switch (name) {
            case "ALL":
                return ImmutableSet.copyOf(values());
            case "SELECT":
                return ImmutableSet.of(SELECT);
            case "INSERT":
                return ImmutableSet.of(INSERT);
            case "UPDATE":
                return ImmutableSet.of(UPDATE);
            case "DELETE":
                return ImmutableSet.of(DELETE);
            case "OWNERSHIP":
                return ImmutableSet.of(OWNERSHIP);
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
}
