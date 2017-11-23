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
package com.facebook.presto.orc;

import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.ExceptionWrappingMetadataReader;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.PostScript;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.OrcReader.checkOrcVersion;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OrcStripeSplit
{
    private final Slice postScript;
    private final Slice fileFooterData;
    private final Optional<Slice> fileMetadataData;

    private final int stripOrdinal;
    private final Optional<Slice> stripeIndexData;
    private final Optional<Slice> stripeFooterData;

    public OrcStripeSplit(Slice postScript,
            Slice fileFooterData,
            Optional<Slice> fileMetadataData,
            int stripOrdinal,
            Optional<Slice> stripeIndexData,
            Optional<Slice> stripeFooterData)
    {
        this.postScript = requireNonNull(postScript, "postScript is null");
        this.fileFooterData = requireNonNull(fileFooterData, "fileFooterData is null");
        this.fileMetadataData = requireNonNull(fileMetadataData, "fileMetadataData is null");
        checkArgument(stripOrdinal >= 0, "stripOrdinal is negative");
        this.stripOrdinal = stripOrdinal;
        this.stripeIndexData = requireNonNull(stripeIndexData, "stripeIndexData is null");
        this.stripeFooterData = requireNonNull(stripeFooterData, "stripeFooterData is null");
    }

    public Slice getPostScript()
    {
        return postScript;
    }

    public Slice getFileFooterData()
    {
        return fileFooterData;
    }

    public Optional<Slice> getFileMetadataData()
    {
        return fileMetadataData;
    }

    public int getStripOrdinal()
    {
        return stripOrdinal;
    }

    public Optional<Slice> getStripeIndexData()
    {
        return stripeIndexData;
    }

    public Optional<Slice> getStripeFooterData()
    {
        return stripeFooterData;
    }

    public OrcRecordReader open(
            Map<Integer, Type> includedColumns,
            OrcDataSource orcDataSource,
            MetadataReader metadataReader,
            OrcPredicate predicate,
            DataSize maxMergeDistance,
            DataSize maxReadSize,
            DataSize maxBlockSize,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryUsage)
            throws IOException
    {
        requireNonNull(orcDataSource, "orcDataSource is null");
        requireNonNull(metadataReader, "metadataReader is null");
        requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        requireNonNull(maxReadSize, "maxReadSize is null");
        requireNonNull(maxBlockSize, "maxBlockSize is null");
        requireNonNull(includedColumns, "includedColumns is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(systemMemoryUsage, "systemMemoryUsage is null");

        metadataReader = new ExceptionWrappingMetadataReader(orcDataSource.getId(), metadataReader);

        // decode the post script
        PostScript postScript = metadataReader.readPostScript(this.postScript);

        // verify this is a supported version
        checkOrcVersion(orcDataSource.getId(), postScript.getVersion());

        int bufferSize = toIntExact(postScript.getCompressionBlockSize());

        // check compression codec is supported
        CompressionKind compressionKind = postScript.getCompression();
        Optional<OrcDecompressor> decompressor = createOrcDecompressor(orcDataSource.getId(), compressionKind, bufferSize);

        // read metadata
        List<StripeStatistics> stripeStats = ImmutableList.of();
        if (fileMetadataData.isPresent()) {
            try (InputStream metadataInputStream = new OrcInputStream(orcDataSource.getId(), fileMetadataData.get().getInput(), decompressor, new AggregatedMemoryContext())) {
                stripeStats = metadataReader.readMetadata(postScript.getHiveWriterVersion(), metadataInputStream).getStripeStatsList();
            }
        }
        Optional<StripeStatistics> stripeStat = Optional.empty();
        if (stripOrdinal < stripeStats.size()) {
            stripeStat = Optional.of(stripeStats.get(stripOrdinal));
        }

        // read footer
        Footer footer;
        try (InputStream footerInputStream = new OrcInputStream(orcDataSource.getId(), fileFooterData.getInput(), decompressor, new AggregatedMemoryContext())) {
            footer = metadataReader.readFooter(postScript.getHiveWriterVersion(), footerInputStream);
        }

        // validate stripe information and augment with preloaded data
        if (footer.getStripes().size() < stripOrdinal) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid strip ordinal %s. File only contains %s stripes", stripOrdinal, footer.getStripes().size());
        }
        StripeInformation stripeInformation = footer.getStripes().get(stripOrdinal);
        if (stripeIndexData.isPresent() && stripeIndexData.get().length() != stripeInformation.getIndexLength()) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid strip handle %s for stripe. Index data size does not match stripe information",
                    this, stripeInformation);
        }
        if (stripeFooterData.isPresent() && stripeFooterData.get().length() != stripeInformation.getFooterLength()) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid strip handle %s for stripe. Footer data size does not match stripe information",
                    this, stripeInformation);
        }
        stripeInformation = stripeInformation.withData(stripeIndexData, stripeFooterData);

        return new OrcRecordReader(
                postScript,
                footer,
                stripeInformation,
                stripeStat,
                includedColumns,
                predicate,
                orcDataSource,
                decompressor,
                hiveStorageTimeZone,
                metadataReader,
                maxMergeDistance,
                maxReadSize,
                maxBlockSize,
                systemMemoryUsage);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("postScript.length", postScript.length())
                .add("fileFooterData.length", fileFooterData.length())
                .add("fileMetadataData.length", fileMetadataData.map(Slice::length).orElse(null))
                .add("stripOrdinal", stripOrdinal)
                .add("stripeIndexData", stripeIndexData.map(Slice::length).orElse(null))
                .add("stripeFooterData", stripeFooterData.map(Slice::length).orElse(null))
                .toString();
    }
}
