package com.facebook.presto.hudi.util;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_META_CLIENT_ERROR;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_SCHEMA_ERROR;

public class HudiUtil
{
    private static final Logger log = Logger.get(HudiUtil.class);
    private static final Cache<Schema, Map<String, Schema.Field>> SCHEMA_FIELD_CACHE =
            CacheBuilder.newBuilder()
                    .maximumSize(1000) // evict after 1000 entries
                    .expireAfterWrite(10, TimeUnit.MINUTES) // expire 10 mins after write
                    .build();

    private HudiUtil() {}

    public static HoodieTableMetaClient buildTableMetaClient(
            ExtendedFileSystem fileSystem,
            String tableName,
            String basePath)
    {
        try {
            StorageConfiguration<Configuration> conf = HadoopFSUtils.getStorageConfWithCopy(fileSystem.getConf());
            return HoodieTableMetaClient.builder()
                    .setConf(conf)
                    .setBasePath(basePath)
                    .build();
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(HUDI_BAD_DATA,
                    "Location of table %s does not contain Hudi table metadata: %s".formatted(tableName, basePath));
        }
        catch (Throwable e) {
            throw new PrestoException(HUDI_META_CLIENT_ERROR,
                    "Unable to load Hudi meta client for table %s (%s)".formatted(tableName, basePath));
        }
    }

    /**
     * Retrieves a field from the given Avro schema by column name.
     * <p>
     * The lookup proceeds in two steps:
     * <ul>
     *   <li>First, attempts an exact match on the column name.</li>
     *   <li>If not found, falls back to a case-insensitive match using a cached lookup table</li>
     * </ul>
     * <p>
     *
     * @param columnName Column name to search for.
     * @param schema Avro {@link org.apache.avro.Schema} in which to search.
     * @return The matching {@link org.apache.avro.Schema.Field}, if found.
     * @throws PrestoException if no field matches the given column name.
     */
    public static Schema.Field getFieldFromSchema(String columnName, Schema schema)
    {
        Schema.Field field = schema.getField(columnName);
        if (field != null) {
            return field;
        }

        try {
            field = SCHEMA_FIELD_CACHE
                    .get(schema, () -> buildFieldLookup(schema)).get(columnName.toLowerCase(Locale.ROOT));
            if (field != null) {
                return field;
            }
        }
        catch (ExecutionException e) {
            throw new PrestoException(HUDI_SCHEMA_ERROR,
                    "Failed to build field lookup for schema", e);
        }

        throw new PrestoException(HUDI_SCHEMA_ERROR,
                "Failed to get column " + columnName + " from table schema");
    }

    private static Map<String, Schema.Field> buildFieldLookup(Schema schema)
    {
        return schema.getFields().stream()
                .collect(Collectors.toMap(
                        f -> f.name().toLowerCase(Locale.ROOT),
                        f -> f));
    }


    public static Schema getLatestTableSchema(HoodieTableMetaClient metaClient, String tableName)
    {
        try {
            HoodieTimer timer = HoodieTimer.start();
            Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
            log.info("Fetched table schema for table %s in %s ms", tableName, timer.endTimer());
            return schema;
        }
        catch (Exception e) {
            // failed to read schema
            throw new PrestoException(HUDI_FILESYSTEM_ERROR, e);
        }
    }
}
