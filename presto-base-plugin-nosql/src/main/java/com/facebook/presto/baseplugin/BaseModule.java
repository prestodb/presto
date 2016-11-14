package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.NodeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseModule implements Module {
    private final NodeManager nodeManager;
    private final BaseConnectorInfo baseConnectorInfo;
    private final Class<? extends BaseConfig> baseConfigClass;
    private final Class<? extends BaseConnector> baseConnectorClass;
    private final Class<? extends BaseMetadata> baseMetadataClass;
    private final Class<? extends BaseSplitManager> baseSplitManagerClass;
    private final Class<? extends BaseRecordSetProvider> baseRecordSetProviderClass;
    private final Class<? extends BaseHandleResolver> baseHandleResolverClass;
    private final Class<? extends BaseProvider> baseProviderClass;

    public BaseModule(
            NodeManager nodeManager,
            BaseConnectorInfo baseConnectorInfo,
            Class<? extends BaseConfig> baseConfigClass,
            Class<? extends BaseConnector> baseConnectorClass,
            Class<? extends BaseMetadata> baseMetadataClass,
            Class<? extends BaseSplitManager> baseSplitManagerClass,
            Class<? extends BaseRecordSetProvider> baseRecordSetProviderClass,
            Class<? extends BaseHandleResolver> baseHandleResolverClass,
            Class<? extends BaseProvider> baseProviderClass
    ) {
        this.nodeManager = requireNonNull(nodeManager);
        this.baseConnectorInfo = requireNonNull(baseConnectorInfo, "baseConnectorInfo is null");
        this.baseConfigClass = requireNonNull(baseConfigClass, "baseConfig is null");
        this.baseConnectorClass = requireNonNull(baseConnectorClass, "baseConnector is null");
        this.baseMetadataClass = requireNonNull(baseMetadataClass, "baseMetadata is null");
        this.baseSplitManagerClass = requireNonNull(baseSplitManagerClass, "baseSplitManager is null");
        this.baseRecordSetProviderClass = requireNonNull(baseRecordSetProviderClass, "baseRecordSetProvider is null");
        this.baseHandleResolverClass = requireNonNull(baseHandleResolverClass, "baseHandleResolver is null");
        this.baseProviderClass = requireNonNull(baseProviderClass, "baseProvider is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(baseConfigClass);

        binder.bind(BaseConnectorInfo.class).toInstance(baseConnectorInfo);
        binder.bind(NodeManager.class).toInstance(nodeManager);

        if(baseConnectorClass != BaseConnector.class) {
            binder.bind(BaseConnector.class).to(baseConnectorClass).in(Scopes.SINGLETON);
        }else{
            binder.bind(BaseConnector.class).in(Scopes.SINGLETON);
        }

        if(baseMetadataClass != BaseMetadata.class){
            binder.bind(BaseMetadata.class).to(baseMetadataClass).in(Scopes.SINGLETON);
        }else{
            binder.bind(BaseMetadata.class).in(Scopes.SINGLETON);
        }

        if(baseSplitManagerClass != BaseSplitManager.class){
            binder.bind(BaseSplitManager.class).to(baseSplitManagerClass).in(Scopes.SINGLETON);
        }else{
            binder.bind(BaseSplitManager.class).in(Scopes.SINGLETON);
        }

        if(baseRecordSetProviderClass != BaseRecordSetProvider.class){
            binder.bind(BaseRecordSetProvider.class).to(baseRecordSetProviderClass).in(Scopes.SINGLETON);
        }else{
            binder.bind(BaseRecordSetProvider.class).in(Scopes.SINGLETON);
        }

        if(baseHandleResolverClass != BaseHandleResolver.class){
            binder.bind(BaseHandleResolver.class).to(baseHandleResolverClass).in(Scopes.SINGLETON);
        }else{
            binder.bind(BaseHandleResolver.class).in(Scopes.SINGLETON);
        }

        if(baseProviderClass != BaseProvider.class){
            binder.bind(BaseProvider.class).to(baseProviderClass).in(Scopes.SINGLETON);
        }else{
            binder.bind(BaseProvider.class).in(Scopes.SINGLETON);
        }
    }
}
