package com.facebook.presto.hive.clustering;

import java.util.List;
import java.util.Map;

// Goal is to extract distribution from table properties.
public final class DistributionExtractor
{
    private DistributionExtractor() {}

    // Extract distributions from table properties.
    // TODO: Attach to Metadata
    static public Map<String, Distribution> extractDistributions()
    {
        return null;
    }

}
