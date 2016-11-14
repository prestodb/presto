package com.facebook.presto.dynamo;

import com.facebook.presto.baseplugin.BasePlugin;

/**
 * Created by amehta on 6/14/16.
 */
public class DynamoPlugin extends BasePlugin {
    @Override
    public void init() {
        setName("dynamo");
        setBaseConfigClass(DynamoConfig.class);
        setBaseProviderClass(DynamoProvider.class);
        setBaseSplitManagerClass(DynamoSplitManager.class);
        setBaseRecordSetProviderClass(DynamoRecordSetProvider.class);
    }
}
