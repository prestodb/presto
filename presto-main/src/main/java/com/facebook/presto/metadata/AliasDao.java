package com.facebook.presto.metadata;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.MetadataUtil.checkTableName;

public interface AliasDao
{
    //
    // TableAliases
    //
    @SqlUpdate("CREATE TABLE IF NOT EXISTS alias (\n" +
            "  src_catalog_name VARCHAR(255) NOT NULL,\n" +
            "  src_schema_name VARCHAR(255) NOT NULL,\n" +
            "  src_table_name VARCHAR(255) NOT NULL,\n" +
            "  dst_catalog_name VARCHAR(255) NOT NULL,\n" +
            "  dst_schema_name VARCHAR(255) NOT NULL,\n" +
            "  dst_table_name VARCHAR(255) NOT NULL,\n" +
            "  UNIQUE(src_catalog_name, src_schema_name, src_table_name),\n" +
            "  UNIQUE(dst_catalog_name, dst_schema_name, dst_table_name)\n" +
            ")")
    void createAliasTable();

    @SqlUpdate("INSERT INTO alias\n" +
            "  (src_catalog_name, src_schema_name, src_table_name, dst_catalog_name, dst_schema_name, dst_table_name)\n" +
            "  VALUES (:srcCatalogName, :srcSchemaName, :srcTableName, :dstCatalogName, :dstSchemaName, :dstTableName)")
    long insertAlias(@BindBean TableAlias alias);

    @SqlUpdate("DELETE FROM alias\n" +
            "  WHERE src_catalog_name = : srcCatalogName AND src_schema_name = :srcSchemaName AND src_table_name = :srcTableName")
    void dropAlias(@BindBean TableAlias alias);

    @SqlQuery("SELECT * FROM alias WHERE src_catalog_name = :catalogName AND src_schema_name = :schemaName AND src_table_name = :tableName")
    @Mapper(TableAlias.TableAliasMapper.class)
    TableAlias getAlias(@Bind("catalogName") String catalogName, @Bind("schemaName") String schemaName, @Bind("tableName") String tableName);

    public static final class Utils
    {
        public static final Logger log = Logger.get(AliasDao.class);

        public static void createTables(AliasDao dao)
        {
            dao.createAliasTable();
        }

        public static QualifiedTableName getAlias(AliasDao dao, QualifiedTableName table)
        {
            checkTableName(table.getCatalogName(), table.getSchemaName(), table.getTableName());

            TableAlias tableAlias = dao.getAlias(table.getCatalogName(), table.getSchemaName(), table.getTableName());
            if (tableAlias == null) {
                return null;
            }

            return new QualifiedTableName(tableAlias.getDstCatalogName(), tableAlias.getDstSchemaName(), tableAlias.getDstTableName());
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
