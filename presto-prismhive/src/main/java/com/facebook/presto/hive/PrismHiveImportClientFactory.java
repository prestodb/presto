package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.google.inject.Inject;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class PrismHiveImportClientFactory
        implements ImportClientFactory
{
    private static final String SOURCE_NAME = "prism";

    private final PrismHiveClient prismHiveClient;

    @Inject
    public PrismHiveImportClientFactory(
            PrismClientProvider prismClientProvider,
            SmcLookup smcLookup,
            HiveClientFactory hiveClientFactory,
            HiveMetastoreClientFactory metastoreClientFactory,
            HiveChunkEncoder hiveChunkEncoder,
            PrismHiveConfig config)
    {
        checkNotNull(prismClientProvider, "prismClientProvider is null");
        checkNotNull(smcLookup, "smcLookup is null");
        checkNotNull(hiveClientFactory, "hiveClientFactory is null");
        checkNotNull(hiveChunkEncoder, "hiveChunkEncoder is null");

        prismHiveClient = new PrismHiveClient(prismClientProvider, smcLookup, hiveClientFactory, metastoreClientFactory, hiveChunkEncoder, config.getCacheTtl());
    }

    @Override
    public ImportClient createClient(String sourceName)
    {
        if (!SOURCE_NAME.equals(sourceName)) {
            return null;
        }

        // Only single instance needed
        return prismHiveClient;
    }
}
