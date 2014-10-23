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
package com.facebook.presto.raptor.storage;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

public enum StorageType
{
    BOOLEAN(BOOLEAN_TYPE_NAME),
    LONG(BIGINT_TYPE_NAME),
    DOUBLE(DOUBLE_TYPE_NAME),
    STRING(STRING_TYPE_NAME),
    BYTES(BINARY_TYPE_NAME);

    private final String hiveTypeName;

    StorageType(String hiveTypeName)
    {
        this.hiveTypeName = checkNotNull(hiveTypeName, "hiveTypeName is null");
    }

    public String getHiveTypeName()
    {
        return hiveTypeName;
    }
}
