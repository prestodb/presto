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
package com.facebook.presto.google.sheets;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.FindReplaceRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.inject.Inject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_ADD_COLUMN_ERROR;
import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR;
import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_CREATE_ERROR;
import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_DROP_ERROR;
import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_FIND_AND_REPLACE_ERROR;
import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_INSERT_ERROR;
import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_METASTORE_ERROR;
import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_TABLE_LOAD_ERROR;
import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_UNKNOWN_TABLE_ERROR;
import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static com.google.common.cache.CacheLoader.from;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SheetsClient
{
    private static final Logger log = Logger.get(SheetsClient.class);

    private static final String APPLICATION_NAME = "Presto google sheets integration";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final String DELIMITER_HASH = "#";
    private static final String DELIMITER_EXCLAMATORY = "!";
    private static final String DELIMITER_COLON = ":";
    private static final String DELIMITER_COMMA = ",";
    private static final String ARCHIVED_TABLE = "Archived";
    private static final String ACTIVE_TABLE = "Active";
    private static final String SHEET_OWNER = "prestosql";
    private static final String MAJOR_DIMENSION_COLUMN = "COLUMNS";
    private static final String FIRST_ROW_ACCESS = "1:1";
    private static final String COLUMN_TYPE_CACHE_KEY = "_column_type";
    private static final String FIELD_SPREED_SHEET_ID = "spreadsheetId";

    private static final List<String> SCOPES = ImmutableList.of(SheetsScopes.SPREADSHEETS);

    private final LoadingCache<String, Optional<String>> tableSheetMappingCache;
    private final LoadingCache<String, List<List<Object>>> sheetDataCache;

    private final String metadataSheetId;
    private final String credentialsFilePath;

    private final Sheets sheetsService;

    private String sheetsRange;
    private String sheetsValueInputOption;
    private final GoogleDriveClient googleDriveClient;
    private final SheetsHelper sheetsHelper;

    @Inject
    public SheetsClient(SheetsConfig config, JsonCodec<Map<String, List<SheetsTable>>> catalogCodec,
                        GoogleDriveClient googleDriveClient, SheetsHelper sheetsHelper)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        this.googleDriveClient = requireNonNull(googleDriveClient, "drive client is null");
        this.sheetsHelper = requireNonNull(sheetsHelper, "sheetHelper is null");
        this.metadataSheetId = config.getMetadataSheetId();
        this.credentialsFilePath = config.getCredentialsFilePath();
        this.sheetsRange = config.getSheetRange();
        this.sheetsValueInputOption = config.getSheetsValueInputOption();

        try {
            this.sheetsService = new Sheets.Builder(newTrustedTransport(), JSON_FACTORY, getCredentials()).setApplicationName(APPLICATION_NAME).build();
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
        long expiresAfterWriteMillis = config.getSheetsDataExpireAfterWrite().toMillis();
        long maxCacheSize = config.getSheetsDataMaxCacheSize();

        this.tableSheetMappingCache = newCacheBuilder(expiresAfterWriteMillis, maxCacheSize)
                .build(new CacheLoader<String, Optional<String>>()
                {
                    @Override
                    public Optional<String> load(String tableName)
                    {
                        return getSheetExpressionForTable(tableName);
                    }
                });

        this.sheetDataCache = newCacheBuilder(expiresAfterWriteMillis, maxCacheSize).build(from(this::readAllValuesFromSheetExpression));
    }

    public Optional<SheetsTable> getTable(String tableName)
    {
        List<List<Object>> values = readAllValues(tableName);
        List<String> columnTypes = Stream.of(tableSheetMappingCache.getUnchecked(tableName + COLUMN_TYPE_CACHE_KEY)
                .get().split(DELIMITER_COMMA)).collect(Collectors.toList());
        if (values.size() > 0) {
            ImmutableList.Builder<SheetsColumn> columns = ImmutableList.builder();
            Set<String> columnNames = new HashSet<>();
            // Assuming 1st line is always header
            List<Object> header = values.get(0);
            int count = 0;
            int idx = 0;
            for (Object column : header) {
                String columnValue = column.toString().toLowerCase(ENGLISH);
                // when empty or repeated column header, adding a placeholder column name
                if (columnValue.isEmpty() || columnNames.contains(columnValue)) {
                    columnValue = "column_" + ++count;
                }
                columnNames.add(columnValue);
                if (idx >= columnTypes.size()) {
                    throw new RuntimeException("Accessing column out of bound:" + columnTypes + " " + idx);
                }
                columns.add(new SheetsColumn(columnValue, sheetsHelper.getType(columnTypes.get(idx++))));
            }
            List<List<Object>> dataValues = values.subList(1, values.size()); // removing header info
            return Optional.of(new SheetsTable(tableName, columns.build(), dataValues));
        }
        return Optional.empty();
    }

    public Set<String> getTableNames()
    {
        ImmutableSet.Builder<String> tables = ImmutableSet.builder();
        try {
            List<List<Object>> tableMetadata = sheetDataCache.getUnchecked(metadataSheetId);
            for (int i = 1; i < tableMetadata.size(); i++) {
                if (tableMetadata.get(i).size() > 0) {
                    // assuming the 3rd field is the field that indicates if this table is "archived" state
                    // (this is the assumed schema of the catalog sheet)
                    if (tableMetadata.get(i).size() >= 3 && isArchived(String.valueOf(tableMetadata.get(i).get(3)))) {
                        continue;
                    }
                    tables.add(String.valueOf(tableMetadata.get(i).get(0)));
                }
            }
            return tables.build();
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw new PrestoException(SHEETS_METASTORE_ERROR, e);
        }
    }

    public List<List<Object>> readAllValues(String tableName)
    {
        try {
            Optional<String> sheetExpression = tableSheetMappingCache.getUnchecked(tableName);
            if (!sheetExpression.isPresent()) {
                throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Sheet expression not found for table " + tableName);
            }
            return sheetDataCache.getUnchecked(sheetExpression.get());
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw new PrestoException(SHEETS_TABLE_LOAD_ERROR, "Error loading data for table: " + tableName, e);
        }
    }

    public Optional<String> getSheetExpressionForTable(String tableName)
    {
        Map<String, Optional<String>> tableSheetMap = getAllTableSheetExpressionMapping();
        if (!tableSheetMap.containsKey(tableName)) {
            return Optional.empty();
        }
        return tableSheetMap.get(tableName);
    }

    private Map<String, Optional<String>> getAllTableSheetExpressionMapping()
    {
        ImmutableMap.Builder<String, Optional<String>> tableSheetMap = ImmutableMap.builder();
        List<List<Object>> data = readAllValuesFromSheetExpression(metadataSheetId);
        // first line is assumed to be sheet header
        for (int i = 1; i < data.size(); i++) {
            if (data.get(i).size() >= 2) {
                String tableId = String.valueOf(data.get(i).get(0));
                String sheetId = String.valueOf(data.get(i).get(1));
                if (data.get(i).size() > 3 && isArchived(String.valueOf(data.get(i).get(3)))) {
                    continue;
                }
                if (data.get(i).size() > 4) {
                    String[] tableColumnTypeInfo = String.valueOf(data.get(i).get(4)).split(DELIMITER_HASH);
                    tableSheetMappingCache.put(tableId + COLUMN_TYPE_CACHE_KEY, Optional.of(tableColumnTypeInfo[1]));
                }
                tableSheetMap.put(tableId.toLowerCase(Locale.ENGLISH), Optional.of(sheetId));
            }
        }
        return tableSheetMap.build();
    }

    private boolean isArchived(String tableStatus)
    {
        String[] tableOptions = tableStatus.split(DELIMITER_HASH);
        if (tableOptions.length < 2) {
            return false;
        }
        return tableOptions[1].equalsIgnoreCase(ARCHIVED_TABLE);
    }

    private Credential getCredentials()
    {
        try (InputStream in = new FileInputStream(credentialsFilePath)) {
            return GoogleCredential.fromStream(in).createScoped(SCOPES);
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_BAD_CREDENTIALS_ERROR, e);
        }
    }

    private List<List<Object>> readAllValuesFromSheetExpression(String sheetExpression)
    {
        try {
            // by default loading up to 10k rows from the first tab of the sheet
            String defaultRange = "$1:$10000";
            String[] tableOptions = sheetExpression.split(DELIMITER_HASH);
            String sheetId = tableOptions[0];
            if (tableOptions.length > 1) {
                defaultRange = tableOptions[1];
            }
            log.debug("Accessing sheet id [%s] with range [%s]", sheetId, defaultRange);
            return sheetsService.spreadsheets().values().get(sheetId, defaultRange).execute().getValues();
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Failed reading data from sheet: " + sheetExpression, e);
        }
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteMillis, long maximumSize)
    {
        return CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).maximumSize(maximumSize);
    }

    public Integer insertIntoSheet(String spreadsheetId, List<List<Object>> rows)
    {
        ValueRange body = new ValueRange()
                .setValues(rows);
        AppendValuesResponse result;
        try {
            result = sheetsService.spreadsheets().values().append(spreadsheetId, sheetsRange, body)
                    .setValueInputOption(sheetsValueInputOption)
                    .execute();
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_INSERT_ERROR, "Error inserting data to sheet: ", e);
        }
        flushSheetDataCache(spreadsheetId);
        return result.getUpdates().getUpdatedCells();
    }

    public void createSheet(String tableName, List<SheetsColumnHandle> columns)
    {
        Spreadsheet spreadsheet = new Spreadsheet()
                .setProperties(new SpreadsheetProperties()
                        .setTitle(tableName));
        try {
            spreadsheet = sheetsService.spreadsheets().create(spreadsheet)
                    .setFields(FIELD_SPREED_SHEET_ID)
                    .execute();
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_CREATE_ERROR, "Error creating sheet: ", e);
        }
        String sheetId = spreadsheet.getSpreadsheetId();
        googleDriveClient.grantPermission(sheetId);
        insertIntoSheet(sheetId, Arrays.asList(
                columns.stream().map(m -> m.getColumnName()).collect(Collectors.toList())));
        List<Object> newMetadata = Arrays.asList(
                tableName,
                spreadsheet.getSpreadsheetId(),
                SHEET_OWNER,
                tableName + DELIMITER_HASH + ACTIVE_TABLE,
                tableName + DELIMITER_HASH + columns.stream()
                        .map(m -> m.getColumnType().getTypeSignature().getBase())
                        .collect(Collectors.joining(DELIMITER_COMMA)));
        insertIntoSheet(metadataSheetId, Arrays.asList(newMetadata));
        flushTableDataCache();
    }

    public void dropSheet(String tableName)
    {
        try {
            findAndReplace(metadataSheetId, tableName
                    + DELIMITER_HASH + ACTIVE_TABLE, tableName + DELIMITER_HASH + ARCHIVED_TABLE);
            flushTableDataCache();
            flushSheetDataCache(metadataSheetId);
        }
        catch (PrestoException e) {
            throw new PrestoException(SHEETS_DROP_ERROR, "Error dropping sheet: ", e);
        }
    }

    public void addColumn(String tableName, List<List<Object>> columns, Type columnType)
    {
        ValueRange body = new ValueRange()
                .setMajorDimension(MAJOR_DIMENSION_COLUMN)
                .setValues(columns);
        try {
            String spreadsheetId = getSheetExpressionForTable(tableName).get();
            List<List<Object>> value = sheetsService.spreadsheets().values()
                    .get(spreadsheetId, sheetsRange + DELIMITER_EXCLAMATORY + FIRST_ROW_ACCESS)
                    .execute().getValues();
            String columnName = getColumnName(value.get(0).size() + 1);
            sheetsService.spreadsheets().values()
                    .append(spreadsheetId,
                            sheetsRange + DELIMITER_EXCLAMATORY + columnName + DELIMITER_COLON + columnName, body)
                    .setValueInputOption(sheetsValueInputOption)
                    .execute();
            addColumnType(tableName, columnType.getTypeSignature().getBase());
            flushTableDataCache();
            flushSheetDataCache(spreadsheetId);
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_ADD_COLUMN_ERROR, "Error while adding a new column to sheet: ", e);
        }
    }

    public void addColumnType(String tableName, String columnType)
    {
        String columnTypes = tableSheetMappingCache.getUnchecked(tableName + COLUMN_TYPE_CACHE_KEY).get();
        findAndReplace(metadataSheetId, tableName + DELIMITER_HASH + columnTypes,
                tableName + DELIMITER_HASH + columnTypes + DELIMITER_COMMA + columnType);
    }

    public String getColumnName(Integer columnSize)
    {
        StringBuilder result = new StringBuilder();
        while (columnSize > 0) {
            columnSize--;
            result.insert(0, (char) ('A' + columnSize % 26));
            columnSize /= 26;
        }
        return result.toString();
    }

    public String getCachedSheetExpressionForTable(String tableName)
    {
        Optional<String> sheetExpression = tableSheetMappingCache.getUnchecked(tableName);
        if (!sheetExpression.isPresent()) {
            throw new PrestoException(SHEETS_UNKNOWN_TABLE_ERROR, "Sheet expression not found for table " + tableName);
        }
        return sheetExpression.get();
    }

    private void findAndReplace(String sheetId, String find, String replace)
    {
        try {
            List<Request> requests = new ArrayList<>();
            requests.add(new Request()
                    .setFindReplace(new FindReplaceRequest().setFind(find).setReplacement(replace).setAllSheets(true)));
            BatchUpdateSpreadsheetRequest body =
                    new BatchUpdateSpreadsheetRequest().setRequests(requests);
            sheetsService.spreadsheets().batchUpdate(sheetId, body).execute();
        }
        catch (IOException e) {
            throw new PrestoException(SHEETS_FIND_AND_REPLACE_ERROR, "Error while adding a new column to sheet: ", e);
        }
    }

    private void flushTableDataCache()
    {
        tableSheetMappingCache.invalidateAll();
    }

    private void flushSheetDataCache(String spreadsheetId)
    {
        sheetDataCache.invalidate(spreadsheetId);
    }

    private static <X extends Throwable> void throwIfInstanceOf(
            Throwable throwable, Class<X> declaredType) throws X
    {
        requireNonNull(throwable);
        if (declaredType.isInstance(throwable)) {
            throw declaredType.cast(throwable);
        }
    }
}
