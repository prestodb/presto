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

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.thrift.TException;

import javax.annotation.concurrent.Immutable;

import java.util.Locale;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;

@Immutable
public class HivePrincipal
{
    private static final String PUBLIC_ROLE_NAME = "public";

    private final String principalName;
    private final PrincipalType principalType;

    public HivePrincipal(String principalName, PrincipalType principalType)
    {
        this.principalType = principalType;

        if (principalType == ROLE) {
            this.principalName = principalName.toLowerCase(Locale.US); // Hive metastore API requires role names to be in lowercase.
        }
        else {
            this.principalName = principalName;
        }
    }

    public static HivePrincipal toHivePrincipal(String grantee)
            throws TException
    {
        if (grantee.equalsIgnoreCase(PUBLIC_ROLE_NAME)) {
            return new HivePrincipal(PUBLIC_ROLE_NAME, ROLE);
        }
        else {
            return new HivePrincipal(grantee, USER);
        }
    }

    public String getPrincipalName()
    {
        return principalName;
    }

    public PrincipalType getPrincipalType()
    {
        return principalType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(principalName, principalType);
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
        HivePrincipal hivePrincipal = (HivePrincipal) o;
        return Objects.equals(principalName, hivePrincipal.principalName) &&
                Objects.equals(principalType, hivePrincipal.principalType);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("principalName", principalName)
                .add("principalType", principalType)
                .toString();
    }
}
