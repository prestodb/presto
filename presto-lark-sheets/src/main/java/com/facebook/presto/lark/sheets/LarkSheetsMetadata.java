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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.lark.sheets.api.LarkSheetsApi;
import com.facebook.presto.lark.sheets.api.LarkSheetsSchema;
import com.facebook.presto.lark.sheets.api.LarkSheetsSchemaStore;
import com.facebook.presto.lark.sheets.api.SheetInfo;
import com.facebook.presto.lark.sheets.api.SpreadsheetInfo;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.lark.sheets.LarkSheetsErrorCode.NOT_PERMITTED;
import static com.facebook.presto.lark.sheets.LarkSheetsErrorCode.SCHEMA_NOT_EXISTS;
import static com.facebook.presto.lark.sheets.LarkSheetsErrorCode.SCHEMA_NOT_READABLE;
import static com.facebook.presto.lark.sheets.LarkSheetsErrorCode.SCHEMA_TOKEN_NOT_PROVIDED;
import static com.facebook.presto.lark.sheets.LarkSheetsErrorCode.SHEET_INVALID_HEADER;
import static com.facebook.presto.lark.sheets.LarkSheetsUtil.mask;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class LarkSheetsMetadata
        implements ConnectorMetadata
{
    static final Pattern INDEX_PATTERN = Pattern.compile("\\$\\d+");
    private final LarkSheetsApi api;
    private final LarkSheetsSchemaStore schemaStore;

    public LarkSheetsMetadata(LarkSheetsApi api, LarkSheetsSchemaStore schemaStore)
    {
        this.api = requireNonNull(api, "api is null");
        this.schemaStore = requireNonNull(schemaStore, "schemaStore is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return StreamSupport.stream(schemaStore.listForUser(session.getUser()).spliterator(), false)
                .map(LarkSheetsSchema::getName)
                .collect(toImmutableList());
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return getVisibleSchema(session, schemaName).isPresent();
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        String schemaName = tableName.getSchemaName();
        LarkSheetsSchema schema = requireVisibleSchema(session, schemaName);
        checkSchemaReadable(schema);
        String token = schema.getToken();
        String sheetName = tableName.getTableName();
        SpreadsheetInfo spreadsheetInfo = api.getMetaInfo(token);
        List<SheetInfo> matched = filterSheets(spreadsheetInfo.getSheets(), sheetName);
        if (matched.isEmpty()) {
            return null;
        }
        else if (matched.size() > 1) {
            throw new PrestoException(LarkSheetsErrorCode.SHEET_NAME_AMBIGUOUS,
                    format("Ambiguous name %s in spreadsheet %s: matched sheets: %s", sheetName, mask(token), matched));
        }
        SheetInfo sheet = matched.get(0);
        return toSheetsTableHandle(sheet);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        String schemaName = tableName.getSchemaName();
        LarkSheetsSchema schema = requireVisibleSchema(session, schemaName);
        checkSchemaReadable(schema);

        if (LarkSheetsSystemTable.requestsSheets(tableName.getTableName())) {
            SpreadsheetInfo metaInfo = api.getMetaInfo(schema.getToken());
            return Optional.of(new LarkSheetsSystemTable(schema.getName(), metaInfo.getSheets()));
        }

        return Optional.empty();
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        LarkSheetsTableHandle tableHandle = (LarkSheetsTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new LarkSheetsTableLayoutHandle(tableHandle));
        return new ConnectorTableLayoutResult(layout, constraint.getSummary());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        LarkSheetsTableHandle sheetsTable = (LarkSheetsTableHandle) table;
        List<LarkSheetsColumnHandle> sheetsColumns = getColumns(sheetsTable);
        List<ColumnMetadata> columnMetadatas = toColumnMetadatas(session, sheetsColumns);
        return new ConnectorTableMetadata(sheetsTable.getSchemaTableName(), columnMetadatas);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LarkSheetsTableHandle sheetsTable = (LarkSheetsTableHandle) tableHandle;
        List<LarkSheetsColumnHandle> columns = getColumns(sheetsTable);
        return columns.stream().collect(toImmutableMap(LarkSheetsColumnHandle::getName, Function.identity()));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (!schemaName.isPresent()) {
            throw new PrestoException(NOT_PERMITTED, "Schema is required to list tables");
        }
        LarkSheetsSchema schema = requireVisibleSchema(session, schemaName.get());
        checkSchemaReadable(schema);
        return api.getMetaInfo(schema.getToken())
                .getSheets()
                .stream()
                .sorted(SheetInfo.indexComparator())
                .map(sheet -> new SchemaTableName(schema.getName(), sheet.getTitle()))
                .collect(toImmutableList());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((LarkSheetsColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        String schemaName = requireNonNull(prefix.getSchemaName(), "prefix.schema is null");
        String prefixTableName = prefix.getTableName();

        LarkSheetsSchema schema = requireVisibleSchema(session, schemaName);
        SpreadsheetInfo metaInfo = api.getMetaInfo(schema.getToken());

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        if (prefixTableName == null) {
            for (SheetInfo sheet : metaInfo.getSheets()) {
                SchemaTableName tableName = new SchemaTableName(schemaName, sheet.getTitle());
                List<LarkSheetsColumnHandle> columnHandles = getColumns(toSheetsTableHandle(sheet));
                List<ColumnMetadata> columnMetadatas = toColumnMetadatas(session, columnHandles);
                builder.put(tableName, columnMetadatas);
            }
        }
        else {
            List<SheetInfo> filtered = filterSheets(metaInfo.getSheets(), prefixTableName);
            if (filtered.size() == 1) {
                for (SheetInfo sheet : filtered) {
                    // Use prefixTableName (other than SheetInfo#getTitle) to create SchemaTableName
                    // in order to make queries like `DESC "@sheetId"` or `DESC "#1"` work
                    SchemaTableName tableName = new SchemaTableName(schemaName, prefixTableName);
                    List<LarkSheetsColumnHandle> columnHandles = getColumns(toSheetsTableHandle(sheet));
                    List<ColumnMetadata> columnMetadatas = toColumnMetadatas(session, columnHandles);
                    builder.put(tableName, columnMetadatas);
                }
            }
            else if (filtered.size() > 1) {
                throw new PrestoException(LarkSheetsErrorCode.SHEET_NAME_AMBIGUOUS,
                        format("Ambiguous name %s in spreadsheet %s: matched sheets: %s", prefixTableName, schema.getToken(), filtered));
            }
        }
        return builder.build();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        String token = LarkSheetsSchemaProperties.getSchemaToken(properties).orElseThrow(
                () -> new PrestoException(SCHEMA_TOKEN_NOT_PROVIDED, "Schema token is required but not provided"));
        boolean publicVisible = LarkSheetsSchemaProperties.isSchemaPublic(properties);
        schemaStore.insert(new LarkSheetsSchema(schemaName, session.getUser(), token, publicVisible));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        LarkSheetsSchema schema = requireVisibleSchema(session, schemaName);
        checkSchemaUpdatable(schema, session.getUser(), "drop");
        schemaStore.delete(schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        LarkSheetsSchema schema = requireVisibleSchema(session, source);
        checkSchemaUpdatable(schema, session.getUser(), "rename");
        schemaStore.rename(source, target);
    }

    private Optional<LarkSheetsSchema> getVisibleSchema(ConnectorSession session, String schemaName)
    {
        return schemaStore.get(schemaName)
                .filter(schema -> schema.isVisibleTo(session.getUser()));
    }

    private LarkSheetsSchema requireVisibleSchema(ConnectorSession session, String schemaName)
    {
        return getVisibleSchema(session, schemaName)
                .orElseThrow(() -> new PrestoException(SCHEMA_NOT_EXISTS,
                        format("Schema %s not exists or not visible", schemaName)));
    }

    private void checkSchemaUpdatable(LarkSheetsSchema schema, String operationUser, String operation)
    {
        if (!schema.getUser().equalsIgnoreCase(operationUser)) {
            throw new PrestoException(NOT_PERMITTED,
                    format("User '%s' is not permitted to perform '%s' on schema '%s'", operationUser, operation, schema.getName()));
        }
    }

    private void checkSchemaReadable(LarkSheetsSchema schema)
    {
        if (!api.isReadable(schema.getToken())) {
            throw new PrestoException(SCHEMA_NOT_READABLE,
                    format("Spreadsheet %s not readable", schema.getToken()));
        }
    }

    private List<LarkSheetsColumnHandle> getColumns(LarkSheetsTableHandle table)
    {
        List<String> header = api.getHeaderRow(table.getSpreadsheetToken(), table.getSheetId(), table.getColumnCount());

        int numColumns = header.size();
        LinkedHashMap<String, LarkSheetsColumnHandle> columns = new LinkedHashMap<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            String rawColumnName = header.get(i);
            if (rawColumnName == null) {
                // Columns without name are ignored
                continue;
            }
            String columnName = rawColumnName.toLowerCase(ENGLISH);
            LarkSheetsColumnHandle column = columns.get(columnName);
            if (column != null) {
                String label = LarkSheetsUtil.columnIndexToColumnLabel(i);
                String prevLabel = LarkSheetsUtil.columnIndexToColumnLabel(column.getIndex());
                throw new PrestoException(SHEET_INVALID_HEADER,
                        format("Duplicated name %s in Column#%s and Column#%s", columnName, prevLabel, label));
            }
            columns.put(columnName, new LarkSheetsColumnHandle(columnName, VARCHAR, i));
        }
        return ImmutableList.copyOf(columns.values());
    }

    private static List<SheetInfo> filterSheets(List<SheetInfo> sheets, String filter)
    {
        requireNonNull(sheets, "sheets is null");
        checkArgument(!isNullOrEmpty(filter), "filter is null or empty");

        char firstChar = filter.charAt(0);
        if (firstChar == '$') {
            // filter by index
            Matcher matcher = INDEX_PATTERN.matcher(filter);
            if (matcher.matches()) {
                int i = Integer.parseInt(filter.substring(1));
                return sheets.stream()
                        .filter(sheet -> sheet.getIndex() == i)
                        .collect(Collectors.toList());
            }
            return ImmutableList.of();
        }
        else if (firstChar == '@') {
            // filter by sheets id
            String sheetId = filter.substring(1);
            return sheets.stream()
                    .filter(sheet -> sheetId.equalsIgnoreCase(sheet.getSheetId()))
                    .collect(Collectors.toList());
        }
        else {
            // filter by title
            return sheets.stream()
                    .filter(sheet -> filter.equalsIgnoreCase(sheet.getTitle()))
                    .collect(Collectors.toList());
        }
    }

    private static LarkSheetsTableHandle toSheetsTableHandle(SheetInfo sheet)
    {
        return new LarkSheetsTableHandle(sheet.getToken(), sheet.getSheetId(), sheet.getTitle(), sheet.getIndex(), sheet.getColumnCount(), sheet.getRowCount());
    }

    private List<ColumnMetadata> toColumnMetadatas(ConnectorSession session, List<LarkSheetsColumnHandle> columnHandles)
    {
        return columnHandles.stream()
                .map(column -> column.toColumnMetadata(normalizeIdentifier(session, column.getName())))
                .collect(toImmutableList());
    }
}
