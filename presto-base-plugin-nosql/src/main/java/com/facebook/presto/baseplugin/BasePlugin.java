package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

/**
 * Created by amehta on 6/13/16.
 */
public abstract class BasePlugin implements Plugin {
    private String name;

    private Class<? extends BaseConfig> baseConfigClass;
    private Class<? extends BaseConnector> baseConnectorClass;
    private Class<? extends BaseMetadata> baseMetadataClass;
    private Class<? extends BaseSplitManager> baseSplitManagerClass;
    private Class<? extends BaseRecordSetProvider> baseRecordSetProviderClass;
    private Class<? extends BaseHandleResolver> baseHandleResolverClass;
    private Class<? extends BaseProvider> baseProviderClass;

    public BasePlugin() {
        setBaseConfigClass(BaseConfig.class);
        setBaseConnectorClass(BaseConnector.class);
        setBaseMetadataClass(BaseMetadata.class);
        setBaseSplitManagerClass(BaseSplitManager.class);
        setBaseRecordSetProviderClass(BaseRecordSetProvider.class);
        setBaseHandleResolverClass(BaseHandleResolver.class);
        setBaseProviderClass(BaseProvider.class);

        //allow custom init
        init();
    }

    public abstract void init();

    public void setName(String name) {
        this.name = name;
    }

    public void setBaseConfigClass(Class<? extends BaseConfig> baseConfigClass) {
        this.baseConfigClass = baseConfigClass;
    }

    public void setBaseConnectorClass(Class<? extends BaseConnector> baseConnectorClass) {
        this.baseConnectorClass = baseConnectorClass;
    }

    public void setBaseMetadataClass(Class<? extends BaseMetadata> baseMetadataClass) {
        this.baseMetadataClass = baseMetadataClass;
    }

    public void setBaseSplitManagerClass(Class<? extends BaseSplitManager> baseSplitManagerClass) {
        this.baseSplitManagerClass = baseSplitManagerClass;
    }

    public void setBaseRecordSetProviderClass(Class<? extends BaseRecordSetProvider> baseRecordSetProviderClass) {
        this.baseRecordSetProviderClass = baseRecordSetProviderClass;
    }

    public void setBaseHandleResolverClass(Class<? extends BaseHandleResolver> baseHandleResolverClass) {
        this.baseHandleResolverClass = baseHandleResolverClass;
    }

    public void setBaseProviderClass(Class<? extends BaseProvider> baseProviderClass) {
        this.baseProviderClass = baseProviderClass;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new BaseConnectorFactory(
                name,
                baseConfigClass,
                baseConnectorClass,
                baseMetadataClass,
                baseSplitManagerClass,
                baseRecordSetProviderClass,
                baseHandleResolverClass,
                baseProviderClass));
    }
}
