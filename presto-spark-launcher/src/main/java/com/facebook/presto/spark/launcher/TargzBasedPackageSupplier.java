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

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

import static com.facebook.presto.spark.launcher.LauncherUtils.checkFile;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TargzBasedPackageSupplier
        implements PackageSupplier
{
    private static final Logger log = Logger.getLogger(TargzBasedPackageSupplier.class.getName());

    private final File packageArchive;

    public TargzBasedPackageSupplier(File packageArchive)
    {
        this.packageArchive = checkFile(requireNonNull(packageArchive, "packageArchive is null"));
    }

    public void deploy(SparkContext context)
    {
        context.addFile(packageArchive.getAbsolutePath());
    }

    @Override
    public File getPrestoSparkPackageDirectory()
    {
        File localPackageFile = packageArchive.exists() ? packageArchive : getLocalFile(packageArchive.getName());
        return ensureDecompressed(localPackageFile, new File(SparkFiles.getRootDirectory()));
    }

    private static File ensureDecompressed(File archive, File outputDirectory)
    {
        String packageDirectoryName = getRootDirectory(archive);
        File packageDirectory = new File(outputDirectory, packageDirectoryName);
        log.info(format("Package directory: %s", packageDirectory));
        if (packageDirectory.exists()) {
            verify(packageDirectory.isDirectory(), "package directory is not a directory: %s", packageDirectory);
            log.info(format("Skipping decompression step as package is already decompressed: %s", packageDirectory));
            return packageDirectory;
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        log.info(format("Decompressing: %s", packageDirectory));
        extractPackage(archive, outputDirectory);
        log.info(format("Decompression took: %sms", stopwatch.elapsed(MILLISECONDS)));
        return packageDirectory;
    }

    private static File getLocalFile(String name)
    {
        String path = requireNonNull(SparkFiles.get(name), "path is null");
        return checkFile(new File(path));
    }

    private static String getRootDirectory(File packageFile)
    {
        try (TarArchiveInputStream archiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(packageFile)))) {
            ArchiveEntry entry = archiveInputStream.getNextEntry();
            if (entry == null) {
                throw new IllegalArgumentException(format("Archive is empty: %s", packageFile));
            }
            Path path = Paths.get(entry.getName());
            return path.getName(0).toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void extractPackage(File packageFile, File outputDirectory)
    {
        try (TarArchiveInputStream archiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(packageFile)))) {
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
}
