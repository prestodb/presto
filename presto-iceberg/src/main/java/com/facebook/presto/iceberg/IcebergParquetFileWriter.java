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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.parquet.ParquetFileWriter;
import com.facebook.presto.parquet.writer.ParquetWriterOptions;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

public class IcebergParquetFileWriter
        extends ParquetFileWriter
        implements IcebergFileWriter
{
    private final Path outputPath;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final MetricsConfig metricsConfig;
    private final Optional<UnaryOperator<Page>> pagePruner;
    // When the file schema has no columns (all-UNKNOWN table), ParquetUtil.fileMetrics
    // fails because the Parquet library's MetadataConverter cannot read a zero-column
    // footer. Track the row count directly so we can return accurate Metrics in that case.
    private final boolean zeroColumnSchema;
    private final AtomicLong rowCount = new AtomicLong();

    public IcebergParquetFileWriter(
            OutputStream outputStream,
            Callable<Void> rollbackAction,
            List<String> fileColumnNames,
            List<Type> fileColumnTypes,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions parquetWriterOptions,
            int[] fileInputColumnIndexes,
            Optional<UnaryOperator<Page>> pagePruner,
            CompressionCodecName compressionCodecName,
            Path outputPath,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            MetricsConfig metricsConfig,
            DateTimeZone writerTimezone,
            String prestoVersion)
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
        this.pagePruner = requireNonNull(pagePruner, "pagePruner is null");
        this.zeroColumnSchema = fileColumnNames.isEmpty();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        super.appendRows(pagePruner.map(pruner -> pruner.apply(dataPage)).orElse(dataPage));
        if (zeroColumnSchema) {
            rowCount.addAndGet(dataPage.getPositionCount());
        }
    }

    @Override
    public Metrics getMetrics()
    {
        if (zeroColumnSchema) {
            // ParquetUtil.fileMetrics fails on a zero-column Parquet file because the Parquet
            // library's MetadataConverter requires at least one schema element. Return just the
            // row count; there are no column statistics to report for an all-UNKNOWN table.
            return new Metrics(rowCount.get(), null, null, null, null);
        }
        return hdfsEnvironment.doAs(hdfsContext.getIdentity().getUser(), () -> ParquetUtil.fileMetrics(new HdfsInputFile(outputPath, hdfsEnvironment, hdfsContext), metricsConfig));
    }
}
