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

package com.facebook.presto.hive.orc;

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.HiveOrcAggregatedMemoryContext;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcLazyReadSmallRanges;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;

public class OrcPageSourceFactoryUtils
{
    private OrcPageSourceFactoryUtils() {}

    public static HdfsOrcDataSource getOrcDataSource(ConnectorSession session, HiveFileSplit fileSplit, HdfsEnvironment hdfsEnvironment, Configuration configuration, HiveFileContext hiveFileContext, FileFormatDataSourceStats stats)
    {
        DataSize maxMergeDistance = getOrcMaxMergeDistance(session);
        DataSize maxBufferSize = getOrcMaxBufferSize(session);
        DataSize streamBufferSize = getOrcStreamBufferSize(session);
        boolean lazyReadSmallRanges = getOrcLazyReadSmallRanges(session);

        Path path = new Path(fileSplit.getPath());
        try {
            FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration).openFile(path, hiveFileContext);
            return new HdfsOrcDataSource(
                    new OrcDataSourceId(fileSplit.getPath()),
                    fileSplit.getFileSize(),
                    maxMergeDistance,
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, fileSplit.getStart(), fileSplit.getLength()), e);
        }
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    public static OrcReader getOrcReader(OrcEncoding orcEncoding, List<HiveColumnHandle> columns, boolean useOrcColumnNames, OrcFileTailSource orcFileTailSource, StripeMetadataSourceFactory stripeMetadataSourceFactory, HiveFileContext hiveFileContext, OrcReaderOptions orcReaderOptions, Optional<EncryptionInformation> encryptionInformation, DwrfEncryptionProvider dwrfEncryptionProvider, OrcDataSource orcDataSource, Path path)
            throws IOException
    {
        DwrfKeyProvider dwrfKeyProvider = new ProjectionBasedDwrfKeyProvider(encryptionInformation, columns, useOrcColumnNames, path);
        OrcReader reader = new OrcReader(
                orcDataSource,
                orcEncoding,
                orcFileTailSource,
                stripeMetadataSourceFactory,
                new HiveOrcAggregatedMemoryContext(),
                orcReaderOptions,
                hiveFileContext.isCacheable(),
                dwrfEncryptionProvider,
                dwrfKeyProvider,
                hiveFileContext.getStats(),
                hiveFileContext.getModificationTime());
        return reader;
    }

    public static PrestoException mapToPrestoException(Exception e, Path path, HiveFileSplit fileSplit)
    {
        // instanceof and class comparison do not work here since they are loaded by different class loaders.
        if (e.getClass().getName().equals(UncheckedExecutionException.class.getName()) && e.getCause() instanceof PrestoException) {
            return (PrestoException) e.getCause();
        }
        if (e instanceof PrestoException) {
            return (PrestoException) e;
        }
        String message = splitError(e, path, fileSplit.getStart(), fileSplit.getLength());
        if (e.getClass().getSimpleName().equals("BlockMissingException")) {
            return new PrestoException(HIVE_MISSING_DATA, message, e);
        }
        return new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
    }
}
