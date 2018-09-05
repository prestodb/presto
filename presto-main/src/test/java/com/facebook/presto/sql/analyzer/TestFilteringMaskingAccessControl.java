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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.transaction.TransactionId;

import java.util.Set;

/**
 * Created by localadmin on 9/11/18.
 */

public class TestFilteringMaskingAccessControl
        extends AllowAllAccessControl
{
    @Override
    public void checkCanSelectFromColumns(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, Set<String> columnNames)
    {
        if (tableName.getObjectName().equals("t3")) {
            if (columnNames.contains("b")) {
                throw new AccessDeniedException("cannot access col b");
            }
        }
    }

    @Override
    public String applyRowFilters(TransactionId transactionId, Identity identity, QualifiedObjectName tableName)
    {
        if (tableName.getObjectName().equals("t1")) {
            return new String("abs(a)>3");
        }
        if (tableName.getObjectName().equals("t2")) {
            return new String("abs(b)>3");
        }

        if (tableName.getObjectName().equals("t3")) {
            return new String("abs(a)>3");
        }

        if (tableName.getObjectName().equals("t5")) {
            return new String("abs(a)>3");
        }

        return null;
    }

    @Override
    public String applyColumnMasking(TransactionId transactionId, Identity identity, QualifiedObjectName tableName, String columnName)
    {
        if (tableName.getObjectName().equals("t1") && columnName.equals("b")) {
            return new String("ceil(b)");
        }

        if (tableName.getObjectName().equals("t3") && columnName.equals("a")) {
            return new String("ceil(a)");
        }

        if (tableName.getObjectName().equals("t5") && columnName.equals("a")) {
            return new String("ceil(a)");
        }
        return null;
    }
}
