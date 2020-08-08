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
package com.facebook.presto.druid.ingestion;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.druid.DruidConfig;
import com.facebook.presto.druid.metadata.DruidColumnInfo;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.druid.DruidErrorCode.DRUID_DEEP_STORAGE_ERROR;
import static java.util.Objects.requireNonNull;

public class DruidPageWriter
{
    public static final JsonFactory JSON_FACTORY = new JsonFactory();
    public static final String DATA_FILE_EXTENSION = ".json.gz";

    private final Configuration hadoopConfiguration;
    private final DruidConfig druidConfig;

    @Inject
    public DruidPageWriter(DruidConfig druidConfig)
    {
        this.druidConfig = requireNonNull(druidConfig, "druidConfig is null");
        this.hadoopConfiguration = druidConfig.readHadoopConfiguration();
    }

    public void append(Page page, DruidIngestionTableHandle tableHandle, Path dataPath)
    {
        Path dataFile = new Path(dataPath, UUID.randomUUID() + DATA_FILE_EXTENSION);
        try (FileSystem fileSystem = dataFile.getFileSystem(hadoopConfiguration);
                FSDataOutputStream outputStream = fileSystem.create(dataFile);
                GZIPOutputStream zipOutputStream = new GZIPOutputStream(outputStream);
                JsonGenerator jsonGen = JSON_FACTORY.createGenerator(zipOutputStream)) {
            jsonGen.writeStartArray();
            for (int position = 0; position < page.getPositionCount(); position++) {
                jsonGen.writeStartObject();
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    DruidColumnInfo column = tableHandle.getColumns().get(channel);
                    Block block = page.getBlock(channel);
                    jsonGen.writeFieldName(column.getColumnName());
                    writeFieldValue(jsonGen, column.getDataType(), block, position);
                }
                jsonGen.writeEndObject();
            }
            jsonGen.writeEndArray();
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_ERROR, "Ingestion failed on " + tableHandle.getTableName(), e);
        }
    }

    private void writeFieldValue(JsonGenerator jsonGen, String dataType, Block block, int position)
            throws IOException
    {
        switch (dataType.toUpperCase()) {
            case "VARCHAR":
            case "OTHER":
                //hyperUnique, approxHistogram Druid column types
                jsonGen.writeString(VARCHAR.getSlice(block, position).toStringUtf8());
                return;
            case "BIGINT":
                jsonGen.writeNumber(BIGINT.getLong(block, position));
                return;
            case "FLOAT":
                jsonGen.writeNumber(REAL.getDouble(block, position));
                return;
            case "DOUBLE":
                jsonGen.writeNumber(DOUBLE.getDouble(block, position));
                return;
            case "TIMESTAMP":
                jsonGen.writeNumber(TIMESTAMP.getLong(block, position));
                return;
            default:
                throw new IllegalArgumentException("unsupported type: " + dataType);
        }
    }
}
