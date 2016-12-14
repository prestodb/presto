package com.facebook.presto.hdfs.util;

import com.facebook.presto.hdfs.HDFSConfig;
import com.facebook.presto.hdfs.exception.ArrayLengthNotMatchException;
import org.apache.hadoop.fs.Path;

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
}
