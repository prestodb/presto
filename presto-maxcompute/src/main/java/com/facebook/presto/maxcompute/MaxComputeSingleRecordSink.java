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
package com.facebook.presto.maxcompute;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.maxcompute.adapter.RecordSink;
import com.facebook.presto.maxcompute.util.MaxComputeWriteUtils;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.aliyun.odps.OdpsType.CHAR;
import static com.aliyun.odps.OdpsType.DECIMAL;
import static com.aliyun.odps.OdpsType.INT;
import static com.aliyun.odps.OdpsType.SMALLINT;
import static com.aliyun.odps.OdpsType.TINYINT;
import static com.aliyun.odps.OdpsType.VARCHAR;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.maxcompute.util.MaxComputeWriteUtils.normalize;
import static com.google.common.base.Strings.padEnd;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class MaxComputeSingleRecordSink
        implements RecordSink
{
    private static final Logger log = Logger.get(MaxComputeSingleRecordSink.class);

    protected final ConnectorSession session;
    protected final String userId;
    protected final String queryId;

    protected final String projectName;
    protected final String tableName;

    protected final TableTunnel tableTunnel;
    protected final MaxComputeClient maxComputeClient;
    protected final MaxComputeInsertTableHandle handle;
    protected final List<Type> columnTypes;
    protected final List<MaxComputeColumnHandle> allColumns;

    protected final MaxComputeColumnHandle[] partitionColumns;
    protected final int[] partitionIndexInAllColumns;
    protected final boolean[] columnIsPartitionIndex;
    private final Table tableMeta;
    private final TableSchema tableSchema;

    private final Map<PartitionHolder, UploadSession> uploadSessions = new HashMap<>();
    private final Map<PartitionHolder, TunnelBufferedWriter> writers = new HashMap<>();
    private final Map<PartitionHolder, Record> records = new HashMap<>();
    private final Object[] currentRow;

    protected int field = -1;

    public MaxComputeSingleRecordSink(MaxComputeInsertTableHandle handle, MaxComputeClient maxComputeClient, ConnectorSession session)
    {
        this.session = session;
        this.queryId = session.getQueryId();
        this.userId = session.getUser();
        log.info("queryId[%s],userId[%s] ODPS creating a new Sink for table: %s.%s", queryId, userId,
                handle.getSchemaName(), handle.getTableName());
        this.maxComputeClient = maxComputeClient;
        this.projectName = handle.getSchemaName();
        this.tableName = handle.getTableName();
        this.handle = handle;
        this.allColumns = handle.getColumnHandleList();
        this.partitionColumns = handle.getColumnHandleList().stream()
                .filter(MaxComputeColumnHandle::isPartitionColumn)
                .sorted(Comparator.comparingInt(MaxComputeColumnHandle::getOriginalColumnIndex))
                .toArray(MaxComputeColumnHandle[]::new);

        partitionIndexInAllColumns = new int[partitionColumns.length];
        for (int i = 0; i < partitionColumns.length; i++) {
            MaxComputeColumnHandle pc = partitionColumns[i];
            int idx = allColumns.indexOf(pc);
            partitionIndexInAllColumns[i] = idx;
        }

        columnIsPartitionIndex = new boolean[allColumns.size()];
        for (int i = 0; i < allColumns.size(); i++) {
            columnIsPartitionIndex[i] = allColumns.get(i).isPartitionColumn();
        }

        this.columnTypes = handle.getTypeList();
        this.currentRow = new Object[allColumns.size()];

        tableTunnel = maxComputeClient.getTableTunnel(session, projectName);
        tableMeta = maxComputeClient.getTableMeta(projectName, tableName);
        tableSchema = tableMeta.getSchema();
        log.info("queryId[%s],userId[%s] ODPS created a new Sink for table: %s.%s", queryId, userId,
                handle.getSchemaName(), handle.getTableName());
    }

    private Pair<TunnelBufferedWriter, Record> getOrCreateUploadSessionIfNeed(PartitionHolder ph)
    {
        TunnelBufferedWriter w = writers.get(ph);
        if (w != null) {
            Record r = records.get(ph);
            return Pair.of(w, r);
        }
        try {
            UploadSession uploadSession;
            if (ph.spec.isPresent()) {
                PartitionSpec sp = ph.spec.get();
                tableMeta.createPartition(sp, true);

                uploadSession = tableTunnel.createUploadSession(projectName, tableName, ph.spec.get());
            }
            else {
                uploadSession = tableTunnel.createUploadSession(projectName, tableName);
            }
            uploadSessions.put(ph, uploadSession);

            TunnelBufferedWriter writer = (TunnelBufferedWriter) uploadSession.openBufferedWriter();
            RetryStrategy retry = new RetryStrategy(6, 4, RetryStrategy.BackoffStrategy.EXPONENTIAL_BACKOFF);
            writer.setRetryStrategy(retry);
            writers.put(ph, writer);

            Record record = uploadSession.newRecord();
            records.put(ph, record);

            return Pair.of(writer, record);
        }
        catch (Exception e) {
            log.error("queryId[%s],userId[%s] ODPS failed to construct single record[%s] sink tunnel: %s",
                    queryId, userId, ph.toString(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beginRecord()
    {
        field = 0;
        log.info("queryId[%s],userId[%s] ODPS record sink begin record.", queryId, userId);
    }

    @Override
    public void finishRecord()
    {
        try {
            getWriterAndInsertCurrentRow();
            log.info("queryId[%s],userId[%s] ODPS record sink finish record.", queryId, userId);
        }
        catch (Exception e) {
            log.error("queryId[%s],userId[%s] ODPS finish record failed: %s", queryId, userId, e.getMessage(), e);
            throw new RuntimeException("Encountered error while writting to ODPS: " + e.getMessage(), e);
        }
    }

    private void getWriterAndInsertCurrentRow()
            throws IOException
    {
        PartitionHolder ph = constructPartitionKey();
        Pair<TunnelBufferedWriter, Record> p = getOrCreateUploadSessionIfNeed(ph);
        TunnelBufferedWriter w = p.getLeft();
        Record r = p.getRight();
        for (int i = 0; i < currentRow.length; i++) {
            if (columnIsPartitionIndex[i]) {
                continue;
            }
            r.set(i, currentRow[i]);
        }
        w.write(r);
    }

    private PartitionHolder constructPartitionKey()
    {
        if (partitionColumns.length == 0) {
            return new PartitionHolder(Optional.empty());
        }
        PartitionSpec ps = new PartitionSpec();
        for (int i = 0; i < partitionIndexInAllColumns.length; i++) {
            int idx = partitionIndexInAllColumns[i];
            MaxComputeColumnHandle pc = partitionColumns[i];
            //mapped name
            String key = pc.getColumnName();
            requireNonNull(currentRow[idx], () -> "The value of partition key[columnName=" + pc.getColumnName()
                    + "] is null");
            String value = String.valueOf(currentRow[idx]);
            ps.set(key, value);
        }
        return new PartitionHolder(Optional.of(ps));
    }

    private void appendColumn(String type, Object t)
    {
        int idx = field;
        try {
            currentRow[field++] = t;
        }
        catch (Exception e) {
            MaxComputeColumnHandle cn = allColumns.get(idx);
            String columnName = cn.getColumnName();
            log.error("queryId[%s],userId[%s] ODPS append%s() failed, type: %s, index: %s, columnName: %s",
                    queryId, userId, type, idx, columnName, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void appendNull()
    {
        appendColumn("Null", null);
    }

    @Override
    public void appendBoolean(boolean value)
    {
        appendColumn("Boolean", value);
    }

    private TypeInfo getMaxcomputeTypeInfo()
    {
        MaxComputeColumnHandle cn = allColumns.get(field);
        int colIdx = cn.getOriginalColumnIndex();
        if (cn.isPartitionColumn()) {
            int partitionColIdx = colIdx - tableSchema.getColumns().size();
            return tableSchema.getPartitionColumn(partitionColIdx).getTypeInfo();
        }
        else {
            return tableSchema.getColumn(colIdx).getTypeInfo();
        }
    }

    @Override
    public void appendLong(long value)
    {
        Type type = columnTypes.get(field);
        TypeInfo typeInfo = getMaxcomputeTypeInfo();
        OdpsType odpsType = typeInfo.getOdpsType();

        Object x = value;
        try {
            if (DATE.equals(type)) {
                x = new Date(TimeUnit.DAYS.toMillis(value));
            }
            else if (TimestampType.TIMESTAMP.equals(type)) {
                x = new Timestamp(value);
            }
            else if (RealType.REAL.equals(type)) {
                x = Float.intBitsToFloat((int) value);
            }

            if (odpsType == TINYINT) {
                appendColumn("Byte", (byte) value);
            }
            else if (odpsType == SMALLINT) {
                appendColumn("Short", (short) value);
            }
            else if (odpsType == INT) {
                appendColumn("Integer", (int) value);
            }
            else {
                appendColumn("Long", x);
            }
        }
        catch (Exception e) {
            errorConvertLog("long", e);
            throw new RuntimeException(e);
        }
    }

    private void errorConvertLog(String fromType, Throwable e)
    {
        Type type = columnTypes.get(field);
        MaxComputeColumnHandle cn = allColumns.get(field);
        String columnName = cn.getColumnName();
        log.error("queryId[%s],userId[%s] failed to convert '%s' to type '%s', index: %s, columnName: %s",
                queryId, userId, fromType, type, field, columnName, e);
    }

    @Override
    public void appendDouble(double value)
    {
        TypeInfo typeInfo = getMaxcomputeTypeInfo();

        if (typeInfo.getOdpsType() == DECIMAL) {
            appendColumn("Decimal", BigDecimal.valueOf(value));
        }
        else {
            appendColumn("Double", value);
        }
    }

    @Override
    public void appendString(byte[] value)
    {
        Object x;
        try {
            TypeInfo typeInfo = getMaxcomputeTypeInfo();
            OdpsType odpsType = typeInfo.getOdpsType();
            String tempStr = new String(value, UTF_8);
            x = tempStr;

            if (odpsType == VARCHAR) {
                VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) typeInfo;
                x = new Varchar(tempStr, varcharTypeInfo.getLength());
                appendColumn("Varchar", x);
            }
            else if (odpsType == CHAR) {
                CharTypeInfo charTypeInfo = (CharTypeInfo) typeInfo;
                String str = padEnd(tempStr, charTypeInfo.getLength(), ' ');
                x = new Char(str, charTypeInfo.getLength());
                appendColumn("Char", x);
            }
            else {
                appendColumn("String", x);
            }
        }
        catch (Exception e) {
            errorConvertLog("byte[]", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void appendDecimal(Object value)
    {
        BigInteger unscaledValue;
        if (value instanceof Slice) {
            Slice slice = (Slice) value;
            unscaledValue = Decimals.decodeUnscaledValue(slice);
        }
        else {
            Long v = (Long) value;
            unscaledValue = BigInteger.valueOf(v);
        }

        Type type = columnTypes.get(field);
        DecimalType decimalType = (DecimalType) type;

        BigDecimal normalize = normalize(new BigDecimal(unscaledValue, decimalType.getScale()), true);
        appendColumn("Decimal", normalize);
    }

    @Override
    public void appendObject(Object value)
    {
        if (value == null) {
            appendColumn("Object", null);
        }
        else {
            MaxComputeColumnHandle cn = allColumns.get(field);
            String columnName = cn.getColumnName();
            Type columnType = cn.getColumnType();

            if (columnType instanceof ArrayType ||
                    columnType instanceof MapType ||
                    columnType instanceof RowType) {
                TypeInfo typeInfo = getMaxcomputeTypeInfo();

                Object objects = MaxComputeWriteUtils.deSerializeObject(columnType, typeInfo, value);
                appendColumn("Object", objects);
            }
            else {
                String msg = "The value is not null, Class=" + value.getClass() +
                        ", index: " + field + ", columnName: " + columnName;
                throw new UnsupportedOperationException(msg);
            }
        }
    }

    @Override
    public Collection<Slice> commit()
    {
        return sinkDone("commit", () -> {
            writers.values().forEach(IOUtils::closeQuietly);
            for (UploadSession uploadSession : uploadSessions.values()) {
                uploadSession.commit();
            }
            // the committer does not need any additional info
            return ImmutableList.of();
        });
    }

    private <T> T sinkDone(@Nonnull String action, @Nonnull Callable<T> callable)
    {
        T o;
        try {
            o = callable.call();
        }
        catch (Exception e) {
            log.error("queryId[%s],userId[%s] failed to %s().", queryId, userId, action);
            throw new RuntimeException("Encountered error while writting to ODPS: " + e.getMessage(), e);
        }

        log.info("queryId[%s],userId[%s] ODPS record sink %s successfully.", queryId, userId, action);
        return o;
    }

    @Override
    public void rollback()
    {
        sinkDone("rollback", () -> {
            writers.values().forEach(IOUtils::closeQuietly);
            return null;
        });
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    private static class PartitionHolder
    {
        static final String DEFAULT_PLACEHOLDER = "__reserved_4_no_partition__";
        static final String DELIMITER = "__@@##$$__";

        final Optional<PartitionSpec> spec;
        final String values;

        public PartitionHolder(@Nonnull Optional<PartitionSpec> spec)
        {
            String x = DEFAULT_PLACEHOLDER;
            if (spec.isPresent()) {
                PartitionSpec rps = spec.get();
                x = rps.keys().stream()
                        .map(rps::get)
                        .collect(joining(DELIMITER));
            }
            values = x;
            this.spec = spec;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionHolder that = (PartitionHolder) o;
            return Objects.equals(values, that.values);
        }

        @Override
        public int hashCode()
        {
            return values.hashCode();
        }

        @Override
        public String toString()
        {
            return "PartitionHolder{" + "spec=" + spec.map(PartitionSpec::toString).orElse("<empty>")
                    + ", values='" + values + '\'' + '}';
        }
    }
}
