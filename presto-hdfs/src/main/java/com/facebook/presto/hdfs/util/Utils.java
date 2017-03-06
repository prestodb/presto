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
package com.facebook.presto.hdfs.util;

import com.facebook.presto.hdfs.HDFSColumnHandle;
import com.facebook.presto.hdfs.HDFSConfig;
import com.facebook.presto.hdfs.exception.ArrayLengthNotMatchException;
import org.apache.hadoop.fs.Path;
import parquet.io.InvalidRecordException;
import parquet.schema.MessageType;
import parquet.schema.Type;

/**
 * presto-root
 *
 * @author Jelly
 */
public final class Utils
{
    private Utils()
    {
    }

    // add path after base path (HDFSConfig.getMetaserverStore)
    public static Path formPath(String dirOrFile)
    {
        String base = HDFSConfig.getMetaserverStore();
        String path = dirOrFile;
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 2);
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return Path.mergePaths(new Path(base), new Path(path));
    }

    public static Path formPath(String dirOrFile1, String dirOrFile2)
    {
        String base = HDFSConfig.getMetaserverStore();
        String path1 = dirOrFile1;
        String path2 = dirOrFile2;
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 2);
        }
        if (!path1.startsWith("/")) {
            path1 = "/" + path1;
        }
        if (path1.endsWith("/")) {
            path1 = path1.substring(0, path1.length() - 2);
        }
        if (!path2.startsWith("/")) {
            path2 = "/" + path2;
        }
        return Path.mergePaths(Path.mergePaths(new Path(base), new Path(path1)), new Path(path2));
    }

    // get database name from database.table[.col]
    public static String getDatabaseName(String databaseTableColName)
    {
        String[] names = databaseTableColName.split("\\.");
        if (names.length < 1) {
            throw new ArrayLengthNotMatchException();
        }
        return names[0];
    }

    // get table name from database.table[.col]
    public static String getTableName(String databaseTableColName)
    {
        String[] names = databaseTableColName.split("\\.");
        if (names.length < 2) {
            throw new ArrayLengthNotMatchException();
        }
        return names[1];
    }

    // get col name from database.table.col
    public static String getColName(String databaseTableColName)
    {
        String[] names = databaseTableColName.split("\\.");
        if (names.length < 3) {
            throw new ArrayLengthNotMatchException();
        }
        return names[2];
    }

    // form concatenated name from database and table
    public static String formName(String database, String table)
    {
        return database + "." + table;
    }

    // from concatenated name from database and table and col
    public static String formName(String database, String table, String col)
    {
        return database + "." + table + "." + col;
    }

    public static Type getParquetType(HDFSColumnHandle column, MessageType messageType)
    {
        if (messageType.containsField(column.getName())) {
            return messageType.getType(column.getName());
        }
        // parquet is case-insensitive, all hdfs-columns get converted to lowercase
        for (Type type : messageType.getFields()) {
            if (type.getName().equalsIgnoreCase(column.getName())) {
                return type;
            }
        }
        return null;
    }

    public static int getFieldIndex(MessageType fileSchema, String name)
    {
        try {
            return fileSchema.getFieldIndex(name);
        }
        catch (InvalidRecordException e) {
            for (Type type : fileSchema.getFields()) {
                if (type.getName().equalsIgnoreCase(name)) {
                    return fileSchema.getFieldIndex(type.getName());
                }
            }
            return -1;
        }
    }
}
