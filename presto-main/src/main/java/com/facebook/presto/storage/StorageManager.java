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

import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;

/**
 * Handles the sources for materialized view.
 * <p/>
 * TODO (after discussion). These methods should be part of the metadata manager and the metadata manager
 * should handle resolution of table handles (and information storage).
 * <p/>
 * This change should be made after the big import client / metadata changes went in.
 */
public interface StorageManager
{
    void insertTableSource(NativeTableHandle tableHandle, QualifiedTableName sourceTableName);

    public QualifiedTableName getTableSource(NativeTableHandle tableHandle);

    public void dropTableSource(NativeTableHandle tableHandle);
}
