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
package com.facebook.presto.nativeworker;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for generating and managing symlink manifest tables.
 * This class provides methods to create symlink manifest files, clean up symlink data,
 * and handle TPCH-related table columns and types.
 */
public final class SymlinkManifestGeneratorUtils
{
    private SymlinkManifestGeneratorUtils() {}

    /**
     * Creates a symlink manifest file at the specified directory.
     * This method collects all file paths under the hiveTableLocation and writes them
     * to a symlink manifest file named "symlink_manifest" inside the specified symlinkManifestDir.
     *
     * @param hiveTableLocation the root directory of the Hive table files
     * @param symlinkManifestDir the directory where the symlink manifest should be created
     * @throws IOException if an I/O error occurs while creating the directories or writing the manifest
     */
    public static void createSymlinkManifest(Path hiveTableLocation, Path symlinkManifestDir) throws IOException
    {
        if (Files.notExists(symlinkManifestDir.getParent())) {
            Files.createDirectory(symlinkManifestDir.getParent());
        }

        if (Files.notExists(symlinkManifestDir)) {
            Files.createDirectory(symlinkManifestDir);
        }

        Path manifestFilePath = symlinkManifestDir.resolve("symlink_manifest");
        try (BufferedWriter writer = Files.newBufferedWriter(manifestFilePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            List<String> fileList = new ArrayList<>();
            collectDataFiles(hiveTableLocation, fileList);

            for (String filePath : fileList) {
                writer.write(filePath);
                writer.write(System.lineSeparator());
            }
        }
    }

    /**
     * Recursively collects all non-hidden file paths in the source directory.
     * If a directory is encountered, the method is called recursively to collect files in subdirectories.
     *
     * @param sourceDir the directory to start collecting files from
     * @param fileList the list where the collected file paths are stored
     * @throws IOException if an I/O error occurs while accessing the directory or files
     */
    private static void collectDataFiles(Path sourceDir, List<String> fileList) throws IOException
    {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceDir)) {
            for (Path entry : stream) {
                if (!Files.isHidden(entry)) {
                    if (Files.isDirectory(entry)) {
                        collectDataFiles(entry, fileList);
                    }
                    else {
                        fileList.add(entry.toString());
                    }
                }
            }
        }
    }

    /**
     * Cleans up (deletes) all files and subdirectories inside the specified directory.
     * This method walks through the directory tree and deletes files in reverse order to ensure
     * that directories are deleted after their contents.
     *
     * @param directory the root directory to clean up
     * @throws IOException if an I/O error occurs during file deletion
     */

    public static void cleanupSymlinkData(Path directory) throws IOException
    {
        MoreFiles.deleteRecursively(directory, RecursiveDeleteOption.ALLOW_INSECURE);
    }
}
