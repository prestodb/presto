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
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.google.sheets.SheetsErrorCode.SHEETS_BAD_CREDENTIALS_ERROR;
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

    private static final List<String> SCOPES = ImmutableList.of(SheetsScopes.SPREADSHEETS_READONLY);

    private final LoadingCache<String, Optional<String>> tableSheetMappingCache;
    private final LoadingCache<String, List<List<Object>>> sheetDataCache;

    private final String metadataSheetId;
    private final String credentialsFilePath;

    private final Sheets sheetsService;

    @Inject
    public SheetsClient(SheetsConfig config, JsonCodec<Map<String, List<SheetsTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        this.metadataSheetId = config.getMetadataSheetId();
        this.credentialsFilePath = config.getCredentialsFilePath();

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
        if (values.size() > 0) {
            ImmutableList.Builder<SheetsColumn> columns = ImmutableList.builder();
            Set<String> columnNames = new HashSet<>();
            // Assuming 1st line is always header
            List<Object> header = values.get(0);
            int count = 0;
            for (Object column : header) {
                String columnValue = column.toString().toLowerCase(ENGLISH);
                // when empty or repeated column header, adding a placeholder column name
                if (columnValue.isEmpty() || columnNames.contains(columnValue)) {
                    columnValue = "column_" + ++count;
                }
                columnNames.add(columnValue);
                columns.add(new SheetsColumn(columnValue, VarcharType.VARCHAR));
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

    private Optional<String> getSheetExpressionForTable(String tableName)
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
                tableSheetMap.put(tableId.toLowerCase(Locale.ENGLISH), Optional.of(sheetId));
            }
        }
        return tableSheetMap.build();
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
            String[] tableOptions = sheetExpression.split("#");
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

    private static <X extends Throwable> void throwIfInstanceOf(
            Throwable throwable, Class<X> declaredType) throws X
    {
        requireNonNull(throwable);
        if (declaredType.isInstance(throwable)) {
            throw declaredType.cast(throwable);
        }
    }
}
