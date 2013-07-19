package com.facebook.presto.storage;

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTableNameMapper;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.concurrent.TimeUnit;

public interface StorageDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS source_table (\n" +
            "  source_table_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  catalog_name VARCHAR(255) NOT NULL,\n" +
            "  schema_name VARCHAR(255) NOT NULL,\n" +
            "  table_name VARCHAR(255) NOT NULL,\n" +
            "  UNIQUE (table_id)\n" +
            ")")
    void createSourceTableTable();

    @SqlUpdate("INSERT INTO source_table (table_id, catalog_name, schema_name, table_name)\n" +
            "VALUES (:tableId, :catalogName, :schemaName, :tableName)")
    @GetGeneratedKeys
    long insertSourceTable(@Bind("tableId") long tableId, @BindBean QualifiedTableName table);

    @SqlUpdate("DELETE FROM source_table WHERE table_id = :tableId")
    int dropSourceTable(@Bind("tableId") long tableId);

    @SqlQuery("SELECT catalog_name, schema_name, table_name\n" +
            "FROM source_table\n" +
            "WHERE table_id = :tableId\n")
    @Mapper(QualifiedTableNameMapper.class)
    QualifiedTableName getSourceTable(@Bind("tableId") long tableId);

    public static class Utils
    {
        public static final Logger log = Logger.get(StorageDao.class);

        public static void createStorageTablesWithRetry(StorageDao dao)
                throws InterruptedException
        {
            Duration delay = new Duration(10, TimeUnit.SECONDS);
            while (true) {
                try {
                    createStorageTables(dao);
                    return;
                }
                catch (UnableToObtainConnectionException e) {
                    log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                    Thread.sleep(delay.toMillis());
                }
            }
        }

        public static void createStorageTables(StorageDao dao)
        {
            dao.createSourceTableTable();
        }
    }
}
