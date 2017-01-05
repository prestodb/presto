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

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FileStorageService
        implements StorageService
{
    private static final Pattern HEX_DIRECTORY = Pattern.compile("[0-9a-f]{2}");
    private static final String FILE_EXTENSION = ".orc";

    private final File baseStorageDir;
    private final File baseStagingDir;

    @Inject
    public FileStorageService(StorageManagerConfig config)
    {
        this(config.getDataDirectory());
    }

    public FileStorageService(File dataDirectory)
    {
        File baseDataDir = requireNonNull(dataDirectory, "dataDirectory is null");
        this.baseStorageDir = new File(baseDataDir, "storage");
        this.baseStagingDir = new File(baseDataDir, "staging");
    }

    @Override
    @PostConstruct
    public void start()
            throws IOException
    {
        deleteDirectory(baseStagingDir);
        createParents(new File(baseStagingDir, "."));
        createParents(new File(baseStorageDir, "."));
    }

    @Override
    public long getAvailableBytes()
    {
        return baseStorageDir.getUsableSpace();
    }

    @PreDestroy
    public void stop()
            throws IOException
    {
        deleteDirectory(baseStagingDir);
    }

    @Override
    public File getStorageFile(UUID shardUuid)
    {
        return getFileSystemPath(baseStorageDir, shardUuid);
    }

    @Override
    public File getStagingFile(UUID shardUuid)
    {
        String name = getFileSystemPath(new File("/"), shardUuid).getName();
        return new File(baseStagingDir, name);
    }

    @Override
    public Set<UUID> getStorageShards()
    {
        ImmutableSet.Builder<UUID> shards = ImmutableSet.builder();
        for (File level1 : listFiles(baseStorageDir, FileStorageService::isHexDirectory)) {
            for (File level2 : listFiles(level1, FileStorageService::isHexDirectory)) {
                for (File file : listFiles(level2, path -> true)) {
                    if (file.isFile()) {
                        uuidFromFileName(file.getName()).ifPresent(shards::add);
                    }
                }
            }
        }
        return shards.build();
    }

    @Override
    public void createParents(File file)
    {
        File dir = file.getParentFile();
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new PrestoException(RAPTOR_ERROR, "Failed creating directories: " + dir);
        }
    }

    /**
     * Generate a file system path for a shard UUID.
     * This creates a three level deep directory structure where the first
     * two levels each contain two hex digits (lowercase) of the UUID
     * and the final level contains the full UUID. Example:
     * <pre>
     * UUID: 701e1a79-74f7-4f56-b438-b41e8e7d019d
     * Path: /base/70/1e/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc
     * </pre>
     * This ensures that files are spread out evenly through the tree
     * while a path can still be easily navigated by a human being.
     */
    public static File getFileSystemPath(File base, UUID shardUuid)
    {
        String uuid = shardUuid.toString().toLowerCase(ENGLISH);
        return base.toPath()
                .resolve(uuid.substring(0, 2))
                .resolve(uuid.substring(2, 4))
                .resolve(uuid + FILE_EXTENSION)
                .toFile();
    }

    private static void deleteDirectory(File dir)
            throws IOException
    {
        if (!dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files == null) {
            throw new IOException("Failed to list directory: " + dir);
        }
        for (File file : files) {
            Files.deleteIfExists(file.toPath());
        }
        Files.deleteIfExists(dir.toPath());
    }

    private static List<File> listFiles(File dir, FileFilter filter)
    {
        File[] files = dir.listFiles(filter);
        if (files == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(files);
    }

    private static boolean isHexDirectory(File file)
    {
        return file.isDirectory() && HEX_DIRECTORY.matcher(file.getName()).matches();
    }

    private static Optional<UUID> uuidFromFileName(String name)
    {
        if (name.endsWith(FILE_EXTENSION)) {
            name = name.substring(0, name.length() - FILE_EXTENSION.length());
            return uuidFromString(name);
        }
        return Optional.empty();
    }

    private static Optional<UUID> uuidFromString(String value)
    {
        try {
            return Optional.of(UUID.fromString(value));
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
