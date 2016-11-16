package com.facebook.presto.hdfs;

import com.facebook.presto.hdfs.metaserver.JDBCMetaServer;
import com.facebook.presto.hdfs.metaserver.MetaServer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class MetaServerModule
implements Module {
    /**
     * Contributes bindings and other configurations for this module to {@code binder}.
     * <p>
     * <p><strong>Do not invoke this method directly</strong> to install submodules. Instead use
     * {@link Binder#install(Module)}, which ensures that {@link Provides provider methods} are
     * discovered.
     *
     * @param binder
     */
    @Override
    public void configure(Binder binder) {
        binder.bind(MetaServer.class).to(JDBCMetaServer.class).in(Scopes.SINGLETON);
    }
}
