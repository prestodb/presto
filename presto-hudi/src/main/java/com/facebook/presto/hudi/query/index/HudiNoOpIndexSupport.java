package com.facebook.presto.hudi.query.index;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.util.Lazy;

/**
 * Noop index support to ensure that MDT enabled split generation is entered.
 */
public class HudiNoOpIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiNoOpIndexSupport.class);

    public HudiNoOpIndexSupport(SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient)
    {
        super(log, schemaTableName, lazyMetaClient);
    }

    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        return true;
    }
}
