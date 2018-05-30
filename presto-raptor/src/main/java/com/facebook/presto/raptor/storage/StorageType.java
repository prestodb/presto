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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

public final class StorageType
{
    public static final StorageType BOOLEAN = new StorageType(BOOLEAN_TYPE_NAME);
    public static final StorageType LONG = new StorageType(BIGINT_TYPE_NAME);
    public static final StorageType DOUBLE = new StorageType(DOUBLE_TYPE_NAME);
    public static final StorageType STRING = new StorageType(STRING_TYPE_NAME);
    public static final StorageType BYTES = new StorageType(BINARY_TYPE_NAME);

    private final String hiveTypeName;

    public static StorageType arrayOf(StorageType elementStorageType)
    {
        return new StorageType(format("%s<%s>", LIST_TYPE_NAME, elementStorageType.getHiveTypeName()));
    }

    public static StorageType mapOf(StorageType keyStorageType, StorageType valueStorageType)
    {
        return new StorageType(format("%s<%s,%s>", MAP_TYPE_NAME, keyStorageType.getHiveTypeName(), valueStorageType.getHiveTypeName()));
    }

    public static StorageType decimal(int precision, int scale)
    {
        return new StorageType(format("%s(%d,%d)", DECIMAL_TYPE_NAME, precision, scale));
    }

    private StorageType(String hiveTypeName)
    {
        this.hiveTypeName = requireNonNull(hiveTypeName, "hiveTypeName is null");
    }

    public String getHiveTypeName()
    {
        return hiveTypeName;
    }
}
