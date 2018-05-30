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
package com.facebook.presto.hive.metastore.file;

import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class PermissionMetadata
{
    private final HivePrivilege permission;
    private final boolean grantOption;

    @JsonCreator
    public PermissionMetadata(
            @JsonProperty("permission") HivePrivilege permission,
            @JsonProperty("grantOption") boolean grantOption)
    {
        this.permission = requireNonNull(permission, "permission is null");
        this.grantOption = requireNonNull(grantOption, "grantOption is null");
    }

    public PermissionMetadata(HivePrivilegeInfo privilegeInfo)
    {
        this.permission = privilegeInfo.getHivePrivilege();
        this.grantOption = privilegeInfo.isGrantOption();
    }

    @JsonProperty
    public HivePrivilege getPermission()
    {
        return permission;
    }

    @JsonProperty
    public boolean isGrantOption()
    {
        return grantOption;
    }

    public HivePrivilegeInfo toHivePrivilegeInfo()
    {
        return new HivePrivilegeInfo(permission, grantOption);
    }
}
