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

import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class AccessControlInfo
{
    private final AccessControl accessControl;
    private final Identity identity;
    private final Optional<TransactionId> transactionId;
    private final AccessControlContext accessControlContext;

    public AccessControlInfo(AccessControl accessControl, Identity identity, Optional<TransactionId> transactionId, AccessControlContext accessControlContext)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.accessControlContext = requireNonNull(accessControlContext, "accessControlContext is null");
    }

    public AccessControl getAccessControl()
    {
        return accessControl;
    }

    public Identity getIdentity()
    {
        return identity;
    }

    public Optional<TransactionId> getTransactionId()
    {
        return transactionId;
    }

    public AccessControlContext getAccessControlContext()
    {
        return accessControlContext;
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

        AccessControlInfo that = (AccessControlInfo) o;
        return Objects.equals(accessControl, that.accessControl) &&
                Objects.equals(identity, that.identity) &&
                Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(accessControlContext, that.accessControlContext);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(accessControl, identity, transactionId, accessControlContext);
    }

    @Override
    public String toString()
    {
        return "AccessControlInfo{" +
                "accessControl=" + accessControl +
                ", identity=" + identity +
                ", transactionId=" + transactionId +
                ", accessControlContext=" + accessControlContext +
                '}';
    }
}
