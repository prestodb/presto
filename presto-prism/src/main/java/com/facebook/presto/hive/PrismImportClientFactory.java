package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.google.inject.Inject;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class PrismImportClientFactory
        implements ImportClientFactory
{
    private static final String SOURCE_NAME = "prism";

    private final PrismClient prismClient;

    @Inject
    public PrismImportClientFactory(
            PrismServiceClientProvider prismServiceClientProvider,
            SmcLookup smcLookup,
            HiveClientFactory hiveClientFactory,
            HiveMetastoreClientFactory metastoreClientFactory,
            HiveChunkEncoder hiveChunkEncoder,
            PrismConfig config)
    {
        checkNotNull(prismServiceClientProvider, "prismServiceClientProvider is null");
        checkNotNull(smcLookup, "smcLookup is null");
        checkNotNull(hiveClientFactory, "hiveClientFactory is null");
        checkNotNull(hiveChunkEncoder, "hiveChunkEncoder is null");

        prismClient = new PrismClient(prismServiceClientProvider,
                smcLookup,
                hiveClientFactory,
                metastoreClientFactory,
                hiveChunkEncoder,
                config.getCacheTtl(),
                new FileSystemCache());
    }

    @Override
    public ImportClient createClient(String sourceName)
    {
        if (!SOURCE_NAME.equals(sourceName)) {
            return null;
        }

        // Only single instance needed
        return prismClient;
    }
}
