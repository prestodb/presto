package com.facebook.presto.hive.util;

import com.google.common.base.Throwables;
import org.apache.hadoop.fs.FileStatus;

import java.lang.invoke.MethodHandle;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

/**
 * Shim to allow using {@link FileStatus} correctly with old versions of Hadoop.
 */
public final class FileStatusUtil
{
    private static final MethodHandle isDirectory = lookupIsDirectory();
    private static final MethodHandle isFile = lookupIsFile();

    private FileStatusUtil() {}

    public static boolean isDirectory(FileStatus status)
    {
        try {
            return (boolean) isDirectory.invokeExact(status);
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }

    public static boolean isFile(FileStatus status)
    {
        try {
            return (boolean) isFile.invokeExact(status);
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }

    private static MethodHandle lookupIsDirectory()
    {
        try {
            return lookup().findVirtual(FileStatus.class, "isDirectory", methodType(boolean.class));
        }
        catch (ReflectiveOperationException ignored) {
        }

        try {
            return lookup().findVirtual(FileStatus.class, "isDir", methodType(boolean.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle lookupIsFile()
    {
        try {
            return lookup().findVirtual(FileStatus.class, "isFile", methodType(boolean.class));
        }
        catch (ReflectiveOperationException ignored) {
        }

        try {
            return lookup().findStatic(FileStatusUtil.class, "isFileShim", methodType(boolean.class, FileStatus.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings({"deprecation", "UnusedDeclaration"})
    private static boolean isFileShim(FileStatus status)
    {
        return !status.isDir();
    }
}
