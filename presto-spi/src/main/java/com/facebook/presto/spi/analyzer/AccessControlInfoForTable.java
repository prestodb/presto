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
package com.facebook.presto.spi.analyzer;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AccessControlInfoForTable
{
    private final AccessControlInfo accessControlInfo;
    private final QualifiedObjectName tableName;

    public AccessControlInfoForTable(AccessControl accessControl, Identity identity, Optional<TransactionId> transactionId, AccessControlContext accessControlContext, QualifiedObjectName tableName)
    {
        this(new AccessControlInfo(accessControl, identity, transactionId, accessControlContext), tableName);
    }

    public AccessControlInfoForTable(AccessControlInfo accessControlInfo, QualifiedObjectName tableName)
    {
        this.accessControlInfo = requireNonNull(accessControlInfo, "accessControlInfo is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public AccessControlInfo getAccessControlInfo()
    {
        return accessControlInfo;
    }

    public QualifiedObjectName getTableName()
    {
        return tableName;
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

        AccessControlInfoForTable that = (AccessControlInfoForTable) o;
        return Objects.equals(accessControlInfo, that.accessControlInfo) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(accessControlInfo, tableName);
    }

    @Override
    public String toString()
    {
        return "AccessControlInfoForTable{" +
                "accessControlInfo=" + accessControlInfo +
                ", tableName=" + tableName +
                '}';
    }
}
