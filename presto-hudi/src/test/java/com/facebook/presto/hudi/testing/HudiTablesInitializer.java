package com.facebook.presto.hudi.testing;

import com.facebook.presto.testing.QueryRunner;
import org.apache.hadoop.fs.Path;

public interface HudiTablesInitializer
{
    void initializeTables(QueryRunner queryRunner, Path externalLocation, String schemaName)
            throws Exception;
}