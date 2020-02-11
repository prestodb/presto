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
package com.facebook.presto.spark.launcher;

import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.isRegularFile;
import static java.nio.file.Files.walk;
import static org.apache.commons.compress.archivers.tar.TarArchiveOutputStream.LONGFILE_GNU;

public class TarGz
{
    private TarGz() {}

    public static void extract(File inputFile, File outputDirectory)
    {
        try (TarArchiveInputStream archiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(inputFile)))) {
            while (true) {
                ArchiveEntry entry = archiveInputStream.getNextEntry();
                if (entry == null) {
                    break;
                }
                File output = new File(outputDirectory, entry.getName());
                if (entry.isDirectory()) {
                    if (output.exists()) {
                        verify(output.isDirectory(), "package directory is not a directory: %s", output);
                        continue;
                    }
                    createDirectories(output.toPath());
                }
                else {
                    File directory = output.getParentFile();
                    if (!directory.exists()) {
                        createDirectories(directory.toPath());
                    }
                    try (OutputStream outputStream = new FileOutputStream(output)) {
                        ByteStreams.copy(archiveInputStream, outputStream);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void create(File directory, File outputFile)
    {
        Path directoryPath = directory.getAbsoluteFile().toPath();
        try (TarArchiveOutputStream archiveOutputStream = new TarArchiveOutputStream(new GzipCompressorOutputStream(new FileOutputStream(outputFile)))) {
            archiveOutputStream.setLongFileMode(LONGFILE_GNU);
            walk(directoryPath).forEach(path -> {
                if (isRegularFile(path)) {
                    try (InputStream inputStream = new FileInputStream(path.toFile())) {
                        String relativePath = directoryPath.relativize(path.toAbsolutePath()).toString();
                        TarArchiveEntry entry = new TarArchiveEntry(directory.getName() + File.separatorChar + relativePath);
                        entry.setSize(Files.size(path));
                        archiveOutputStream.putArchiveEntry(entry);
                        ByteStreams.copy(inputStream, archiveOutputStream);
                        archiveOutputStream.closeArchiveEntry();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String getRootDirectoryName(File archive)
    {
        try (TarArchiveInputStream archiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(archive)))) {
            ArchiveEntry entry = archiveInputStream.getNextEntry();
            if (entry == null) {
                throw new IllegalArgumentException(format("Archive is empty: %s", archive));
            }
            Path path = Paths.get(entry.getName());
            return path.getName(0).toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
