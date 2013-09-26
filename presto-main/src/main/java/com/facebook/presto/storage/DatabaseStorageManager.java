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
package com.facebook.presto.storage;

import com.facebook.presto.metadata.ForMetadata;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.storage.StorageDao.Utils;
import com.google.inject.Inject;
import org.skife.jdbi.v2.IDBI;

import static com.google.common.base.Preconditions.checkNotNull;

public class DatabaseStorageManager
        implements StorageManager
{
    private final IDBI dbi;
    private final StorageDao dao;

    @Inject
    DatabaseStorageManager(@ForMetadata IDBI dbi)
            throws InterruptedException
    {
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(StorageDao.class);

        Utils.createStorageTablesWithRetry(dao);
    }

    @Override
    public void insertTableSource(NativeTableHandle tableHandle,
            QualifiedTableName sourceTableName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(sourceTableName, "sourceTableName is null");

        dao.insertSourceTable(tableHandle.getTableId(), sourceTableName);
    }

    @Override
    public QualifiedTableName getTableSource(NativeTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        return dao.getSourceTable(tableHandle.getTableId());
    }

    @Override
    public void dropTableSource(NativeTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        dao.dropSourceTable(tableHandle.getTableId());
    }
}
