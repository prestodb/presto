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

package com.facebook.presto.hudi;

import com.facebook.presto.common.FileFormatDataSourceStats;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetPageSource;
import com.facebook.presto.parquet.ParquetPageSourceProvider;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableFormatColumnHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.parquet.ParquetPageSourceProvider.createCommonParquetPageSource;
import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetTypeByName;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;
import static org.apache.parquet.io.ColumnIOConverter.constructField;

class HudiParquetPageSources
{
    private HudiParquetPageSources() {}

    public static ConnectorPageSource createParquetPageSource(
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<HudiColumnHandle> regularColumns,
            DataSize maxReadBlockSize,
            boolean batchReaderEnabled,
            boolean verificationEnabled,
            TupleDomain<HudiColumnHandle> effectivePredicate,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            boolean columnIndexFilterEnabled)
    {
        return createCommonParquetPageSource(hdfsEnvironment,
               user,
               configuration,
               path,
               start,
               length,
               regularColumns,
               null,
               maxReadBlockSize,
               batchReaderEnabled,
               verificationEnabled,
               typeManager,
               effectivePredicate,
               fileFormatDataSourceStats,
               columnIndexFilterEnabled,
               new HudiPageSourceCommons(),
               false,
               new RuntimeStats());
    }

    static class HudiPageSourceCommons
            extends ParquetPageSourceProvider.PageSourceCommons
    {
        public Optional<List<org.apache.parquet.schema.Type>> getParquetFields(
                List<? extends TableFormatColumnHandle> columns,
                TypeManager typeManager,
                MessageType fileSchema,
                SchemaTableName tableName,
                Path path)
        {
            List<Type> parquetFields = columns.stream()
                    .map(column -> getParquetTypeByName(column.getName(), fileSchema))
                    .collect(toList());
            return Optional.of(parquetFields);
        }

        public ParquetPageSource getPageSource(
                ParquetReader parquetReader,
                List<? extends TableFormatColumnHandle> columns,
                MessageColumnIO messageColumnIO,
                MessageType fileSchema,
                SchemaTableName tableName,
                Path path,
                TypeManager typeManager,
                Optional<List<org.apache.parquet.schema.Type>> parquetFields,
                boolean useParquetColumnNames,
                RuntimeStats runtimeStats)
        {
            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<com.facebook.presto.common.type.Type> prestoTypes = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                HudiColumnHandle column = (HudiColumnHandle) columns.get(columnIndex);
                namesBuilder.add(column.getName());
                Type parquetField = parquetFields.get().get(columnIndex);

                com.facebook.presto.common.type.Type prestoType = column.getHiveType().getType(typeManager);

                prestoTypes.add(prestoType);

                if (parquetField == null) {
                    internalFields.add(Optional.empty());
                }
                else {
                    internalFields.add(constructField(prestoType, messageColumnIO.getChild(parquetField.getName())));
                }
            }

            return new ParquetPageSource(parquetReader, prestoTypes.build(), internalFields.build(), namesBuilder.build(), runtimeStats);
        }

        public MessageType getRequestedSchema(
                List<? extends TableFormatColumnHandle> columns,
                TypeManager typeManager,
                MessageType fileSchema,
                SchemaTableName tableName,
                Path path,
                Optional<List<org.apache.parquet.schema.Type>> parquetFields,
                boolean useParquetColumnNames)
        {
            return new MessageType(fileSchema.getName(), parquetFields.get().stream().filter(Objects::nonNull).collect(toImmutableList()));
        }
    }
}
