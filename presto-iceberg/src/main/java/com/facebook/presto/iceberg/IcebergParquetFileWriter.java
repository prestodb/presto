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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.parquet.ParquetFileWriter;
import com.facebook.presto.parquet.writer.ParquetWriterOptions;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class IcebergParquetFileWriter
        extends ParquetFileWriter
        implements IcebergFileWriter
{
    private final Path outputPath;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final MetricsConfig metricsConfig;
    private final Set<Integer> geospatialFieldIds;

    public IcebergParquetFileWriter(
            OutputStream outputStream,
            Callable<Void> rollbackAction,
            List<String> fileColumnNames,
            List<Type> fileColumnTypes,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions parquetWriterOptions,
            int[] fileInputColumnIndexes,
            CompressionCodecName compressionCodecName,
            Path outputPath,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            MetricsConfig metricsConfig,
            DateTimeZone writerTimezone,
            String prestoVersion,
            Set<Integer> geospatialFieldIds)
    {
        super(outputStream,
                rollbackAction,
                fileColumnNames,
                fileColumnTypes,
                messageType,
                primitiveTypes,
                parquetWriterOptions,
                fileInputColumnIndexes,
                compressionCodecName,
                writerTimezone,
                prestoVersion);
        this.outputPath = requireNonNull(outputPath, "outputPath is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.metricsConfig = requireNonNull(metricsConfig, "metricsConfig is null");
        this.geospatialFieldIds = ImmutableSet.copyOf(requireNonNull(geospatialFieldIds, "geospatialFieldIds is null"));
    }

    @Override
    public Metrics getMetrics()
    {
        Metrics metrics = hdfsEnvironment.doAs(
                hdfsContext.getIdentity().getUser(),
                () -> ParquetUtil.fileMetrics(new HdfsInputFile(outputPath, hdfsEnvironment, hdfsContext), metricsConfig));
        return dropGeospatialBounds(metrics);
    }

    /**
     * Removes the lower and upper bounds of geospatial columns.
     *
     * <p>Iceberg computes bounds for a geospatial column as if it were binary, because its
     * Parquet binding does not recognize the geometry and geography logical types (as of
     * 1.11.0) and so reads the column back as binary. The resulting bounds are byte
     * comparisons of well-known binary, while the Iceberg specification gives a geospatial
     * field's bounds geospatial meaning. Reporting them would let a reader that follows the
     * specification prune on a value that does not mean what it appears to, dropping rows
     * from otherwise correct queries, so the bounds are omitted until Iceberg can produce
     * real geospatial bounds. Counts and sizes stay: they are type independent.
     */
    private Metrics dropGeospatialBounds(Metrics metrics)
    {
        if (geospatialFieldIds.isEmpty()) {
            return metrics;
        }
        return new Metrics(
                metrics.recordCount(),
                metrics.columnSizes(),
                metrics.valueCounts(),
                metrics.nullValueCounts(),
                metrics.nanValueCounts(),
                withoutGeospatialFields(metrics.lowerBounds()),
                withoutGeospatialFields(metrics.upperBounds()));
    }

    private Map<Integer, ByteBuffer> withoutGeospatialFields(Map<Integer, ByteBuffer> bounds)
    {
        if (bounds == null) {
            return null;
        }
        return bounds.entrySet().stream()
                .filter(entry -> !geospatialFieldIds.contains(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
