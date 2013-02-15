package com.facebook.presto.hive;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class PrismConfig
{
    private Duration cacheTtl = new Duration(1, TimeUnit.HOURS);
    private String prismSmcTier = "prism.nssr";

    @NotNull
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("prism.cache-ttl")
    public PrismConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    @NotNull
    public String getPrismSmcTier()
    {
        return prismSmcTier;
    }

    @Config("prism.smc-tier")
    public PrismConfig setPrismSmcTier(String prismSmcTier)
    {
        this.prismSmcTier = prismSmcTier;
        return this;
    }
}
