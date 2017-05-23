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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.PrivilegeInfo;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege.DELETE;
import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege.INSERT;
import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege.OWNERSHIP;
import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege.SELECT;
import static com.facebook.presto.raptor.metadata.RaptorPrivilegeInfo.RaptorPrivilege.UPDATE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RaptorPrivilegeInfo
{
    private final RaptorPrivilege raptorPrivilege;
    private final boolean grantOption;
    public RaptorPrivilegeInfo(RaptorPrivilege raptorPrivilege, boolean grantOption)
    {
        this.raptorPrivilege = requireNonNull(raptorPrivilege, "raptorPrivilege is null");
        this.grantOption = grantOption;
    }

    public static RaptorPrivilege toRaptorPrivilege(Privilege privilege)
    {
        switch (privilege) {
            case SELECT:
                return SELECT;
            case INSERT:
                return INSERT;
            case DELETE:
                return DELETE;
            case UPDATE:
                return UPDATE;
        }
        return null;
    }

    public static Set<RaptorPrivilegeInfo> fromMaskValue(long maskValue, boolean grantOption)
    {
        ImmutableSet.Builder<RaptorPrivilegeInfo> privileges = ImmutableSet.builder();
        if (SELECT.contained(maskValue)) {
            privileges.add(new RaptorPrivilegeInfo(SELECT, grantOption));
        }
        if (DELETE.contained(maskValue)) {
            privileges.add(new RaptorPrivilegeInfo(DELETE, grantOption));
        }
        if (INSERT.contained(maskValue)) {
            privileges.add(new RaptorPrivilegeInfo(INSERT, grantOption));
        }
        if (UPDATE.contained(maskValue)) {
            privileges.add(new RaptorPrivilegeInfo(UPDATE, grantOption));
        }
        if (OWNERSHIP.contained(maskValue)) {
            privileges.add(new RaptorPrivilegeInfo(OWNERSHIP, grantOption));
        }

        return privileges.build();
    }

    public static long toMaskValue(Set<RaptorPrivilegeInfo> privileges)
    {
        long mask = 0;
        for (RaptorPrivilegeInfo privilege : privileges) {
            mask |= privilege.getRaptorPrivilege().getMaskValue();
        }

        return mask;
    }

    public RaptorPrivilege getRaptorPrivilege()
    {
        return raptorPrivilege;
    }

    public boolean isGrantOption()
    {
        return grantOption;
    }

    public Set<PrivilegeInfo> toPrivilegeInfo()
    {
        switch (getRaptorPrivilege()) {
            case SELECT:
                return ImmutableSet.of(new PrivilegeInfo(Privilege.SELECT, isGrantOption()));
            case INSERT:
                return ImmutableSet.of(new PrivilegeInfo(Privilege.INSERT, isGrantOption()));
            case DELETE:
                return ImmutableSet.of(new PrivilegeInfo(Privilege.DELETE, isGrantOption()));
            case UPDATE:
                return ImmutableSet.of(new PrivilegeInfo(Privilege.UPDATE, isGrantOption()));
            case OWNERSHIP:
                return Arrays.asList(Privilege.values()).stream()
                        .map(privilege -> new PrivilegeInfo(privilege, Boolean.TRUE))
                        .collect(Collectors.toSet());
        }
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(raptorPrivilege, grantOption);
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
        RaptorPrivilegeInfo ratporPrivilegeInfo = (RaptorPrivilegeInfo) o;
        return Objects.equals(raptorPrivilege, ratporPrivilegeInfo.raptorPrivilege) &&
                Objects.equals(grantOption, ratporPrivilegeInfo.grantOption);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("privilege", raptorPrivilege)
                .add("grantOption", grantOption)
                .toString();
    }

    public enum RaptorPrivilege
    {
        SELECT(1),      // 0001
        INSERT(2),      // 0010
        UPDATE(4),      // 0100
        DELETE(8),      // 1000
        OWNERSHIP(31);  // 1111

        private final long maskValue;

        RaptorPrivilege(long maskValue)
        {
            this.maskValue = maskValue;
        }

        public long getMaskValue()
        {
            return maskValue;
        }

        public boolean contained(long maskValue)
        {
            return (this.maskValue & maskValue) == this.maskValue;
        }
    }
}
