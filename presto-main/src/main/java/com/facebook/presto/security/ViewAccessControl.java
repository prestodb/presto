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
package com.facebook.presto.security;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.transaction.TransactionId;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ViewAccessControl
        extends DenyAllAccessControl
{
    private final AccessControl delegate;

    public ViewAccessControl(AccessControl delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        delegate.checkCanCreateViewWithSelectFromColumns(transactionId, identity, context, tableName, columnNames);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(TransactionId transactionId, Identity identity, AccessControlContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        delegate.checkCanCreateViewWithSelectFromColumns(transactionId, identity, context, tableName, columnNames);
    }
}
