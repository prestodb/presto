package com.facebook.presto.metadata;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface AliasDao
{
    //
    // TableAliases
    //
    @SqlUpdate("CREATE TABLE IF NOT EXISTS alias (\n" +
            "  source_connector_id VARCHAR(255) NOT NULL,\n" +
            "  source_schema_name VARCHAR(255) NOT NULL,\n" +
            "  source_table_name VARCHAR(255) NOT NULL,\n" +
            "  destination_connector_id VARCHAR(255) NOT NULL,\n" +
            "  destination_schema_name VARCHAR(255) NOT NULL,\n" +
            "  destination_table_name VARCHAR(255) NOT NULL,\n" +
            "  UNIQUE(destination_connector_id, destination_schema_name, destination_table_name)\n" +
            ")")
    void createAliasTable();

    @SqlUpdate("INSERT INTO alias\n" +
            "  (source_connector_id, source_schema_name, source_table_name, destination_connector_id, destination_schema_name, destination_table_name)\n" +
            "  VALUES (:sourceConnectorId, :sourceSchemaName, :sourceTableName, :destinationConnectorId, :destinationSchemaName, :destinationTableName)")
    long insertAlias(@BindBean TableAlias alias);

    @SqlUpdate("DELETE FROM alias\n" +
            "  WHERE source_connector_id = :sourceConnectorId AND source_schema_name = :sourceSchemaName AND source_table_name = :sourceTableName")
    void dropAlias(@BindBean TableAlias alias);

    @SqlQuery("SELECT * FROM alias WHERE source_connector_id = :connectorId AND source_schema_name = :schemaName AND source_table_name = :tableName")
    @Mapper(TableAlias.TableAliasMapper.class)
    TableAlias getAlias(@Bind("connectorId") String connectorId, @Bind("schemaName") String schemaName, @Bind("tableName") String tableName);

    @SqlQuery("SELECT * FROM alias")
    @Mapper(TableAlias.TableAliasMapper.class)
    List<TableAlias> getAliases();

    public static final class Utils
    {
        public static final Logger log = Logger.get(AliasDao.class);

        public static void createTables(AliasDao dao)
        {
            dao.createAliasTable();
        }

        public static void createTablesWithRetry(AliasDao dao)
                throws InterruptedException
        {
            Duration delay = new Duration(10, TimeUnit.SECONDS);
            while (true) {
                try {
                    createTables(dao);
                    return;
                }
                catch (UnableToObtainConnectionException e) {
                    log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                    Thread.sleep((long) delay.toMillis());
                }
            }
        }

    }
}
