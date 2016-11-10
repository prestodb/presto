package com.facebook.presto.baseplugin;

import com.facebook.presto.baseplugin.cache.BaseRecordBatch;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public abstract class BaseProvider {
    private final BaseConfig config;
    private final Map<SchemaTableName, List<BaseColumnHandle>> tableMap;

    private final LoadingCache<BaseQuery, BaseRecordBatch> recordCache;

    public BaseProvider(BaseConfig config) {
        this.config = requireNonNull(config, "config is null");
        this.tableMap = new HashMap<>();

        this.recordCache = CacheBuilder.newBuilder().maximumSize(100).expireAfterAccess(2, TimeUnit.MINUTES).build(new CacheLoader<BaseQuery, BaseRecordBatch>() {
            @Override
            public BaseRecordBatch load(BaseQuery key) throws Exception {
                return getRecordBatchForQueryFromSource(key);
            }
        });
    }

    public BaseConfig getConfig() {
        return config;
    }

    public LoadingCache<BaseQuery, BaseRecordBatch> getRecordCache() {
        return recordCache;
    }

    /**
     * Gets the schema names for the source
     * @param session contains query specific meta-information
     * @return a list of schema names for the source
     */
    public List<String> listSchemaNames(ConnectorSession session){
        return ImmutableList.of(config.getDefaultSchemaName(Optional.of(session)));
    }

    /**
     * Gets a list of columns for a table/schema
     * @param session contains query specific meta-information
     * @param name the schema/table for which to get columns
     * @return columns corresponding to a table/schema
     */
    public List<BaseColumnHandle> getTableColumns(ConnectorSession session, SchemaTableName name) {
        return tableMap.computeIfAbsent(name, n -> generateTableColumns(session, n));
    }

    /**
     * Gets a list of columnMetadata for a schema/table
     * @param session contains query specific meta-information
     * @param schemaTableName the schema/table for which to get columns
     * @return the columnMetadata's corresponding to a schema/table
     */
    public List<ColumnMetadata> listColumnMetadata(ConnectorSession session, SchemaTableName schemaTableName){
        return getTableColumns(session, schemaTableName).stream().map(x -> new ColumnMetadata(x.getColumnName(), x.getColumnType())).collect(Collectors.toList());
    }

    /**
     * Gets a map of columnName -> columnHandles for a schema/table
     * @param session contains query specific meta-information
     * @param tableName the schema/table for which to get columns
     * @return the columnName -> columnHandle corresponding to a schema/table
     */
    public Map<String, ColumnHandle> getMappedColumnHandlesForSchemaTable(ConnectorSession session, SchemaTableName tableName){
        return getTableColumns(session, tableName).stream().collect(Collectors.toMap(BaseColumnHandle::getColumnName, Function.identity()));
    }

    /**
     * attempts to get a recordBatch from custom logic.  If that returns an EMPTY_BATCH, hits source
     * @param query the request for data
     * @return the recordBatch
     */
    public BaseRecordBatch getRecordBatchForQuery(BaseQuery query){
        if(config.getCacheEnabled()){
            try {
                return recordCache.get(query);
            }catch (ExecutionException e){
                e.printStackTrace();
            }
        }
        return getRecordBatchForQueryFromSource(query);
    }

    /**
     * Gets a recordBatch corresponding to the state of a given query
     * @param key the query that represents a request for data
     * @return the recordBatch corresponding to the state of the given query
     */
    public abstract BaseRecordBatch getRecordBatchForQueryFromSource(BaseQuery key);

    public abstract List<BaseColumnHandle> generateTableColumns(ConnectorSession session, SchemaTableName tableName);

    public abstract List<SchemaTableName> listTableNames(ConnectorSession session, Optional<String> schemaName);
}
