package com.facebook.presto.hive;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.hadoop.compression.lzo.GPLNativeCodeLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.NativeCodeLoader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@SuppressWarnings({"StaticNonFinalField", "RedundantFieldInitialization"})
public final class HadoopNative
{
    private static boolean loaded = false;
    private static Throwable error = null;

    private HadoopNative() {}

    public static synchronized void requireHadoopNative()
    {
        if (loaded) {
            return;
        }
        if (error != null) {
            throw new RuntimeException("failed to load Hadoop native library", error);
        }
        try {
            loadLibrary("hadoop");
            Field field = NativeCodeLoader.class.getDeclaredField("nativeCodeLoaded");
            field.setAccessible(true);
            field.set(null, true);

            loadLibrary("gplcompression");
            loadLibrary("lzo2");
            field = GPLNativeCodeLoader.class.getDeclaredField("nativeLibraryLoaded");
            field.setAccessible(true);
            field.set(null, true);

            // Use reflection to HACK fix caching bug in CodecPool
            // This hack works from a concurrency perspective in this codebase
            // because we assume that all threads that will access the CodecPool will
            // have already been synchronized by calling requireHadoopNative() at some point.
            field = CodecPool.class.getDeclaredField("COMPRESSOR_POOL");
            setFinalStatic(field, new HackListMap<>());
            field = CodecPool.class.getDeclaredField("DECOMPRESSOR_POOL");
            setFinalStatic(field, new HackListMap<>());

            CompressionCodecFactory.getCodecClasses(new Configuration());

            loaded = true;
        }
        catch (Throwable t) {
            error = t;
            throw new RuntimeException("failed to load Hadoop native library", error);
        }
    }

    private static void setFinalStatic(Field field, Object newValue)
            throws Exception
    {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, newValue);
    }

    private static void loadLibrary(String name)
            throws IOException
    {
        URL url = Resources.getResource(HadoopNative.class, getLibraryPath(name));
        File file = File.createTempFile(name, null);
        file.deleteOnExit();
        Files.copy(Resources.newInputStreamSupplier(url), file);
        System.load(file.getAbsolutePath());
    }

    private static String getLibraryPath(String name)
    {
        return "/nativelib/" + getPlatform() + "/" + System.mapLibraryName(name);
    }

    private static String getPlatform()
    {
        String name = System.getProperty("os.name");
        String arch = System.getProperty("os.arch");
        return (name + "-" + arch).replace(' ', '_');
    }

    private static class HackListMap<K, V>
            extends AbstractMap<K, List<? extends V>>
    {
        @Override
        public Set<Entry<K, List<? extends V>>> entrySet()
        {
            return Collections.emptySet();
        }

        @Override
        public List<? extends V> put(K key, List<? extends V> value)
        {
            return null;
        }

        @Override
        public List<? extends V> get(Object key)
        {
            return new ArrayList<>();
        }
    }
}
