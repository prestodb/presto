package com.facebook.presto.hive.security;

import com.facebook.presto.plugin.ranger.security.ranger.RangerBasedAccessControl;
import com.facebook.presto.plugin.ranger.security.ranger.RangerBasedAccessControlConfig;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class RangerdSecurityModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(ConnectorAccessControl.class).to(RangerBasedAccessControl.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(RangerBasedAccessControlConfig.class);
    }
}
