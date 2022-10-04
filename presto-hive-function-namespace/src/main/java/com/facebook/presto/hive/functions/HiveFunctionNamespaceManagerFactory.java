/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.hive.functions;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.function.FunctionHandleResolver;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceManagerContext;
import com.facebook.presto.spi.function.FunctionNamespaceManagerFactory;
import com.google.inject.Injector;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.llap.security.LlapSigner;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;

import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.HIVE_FUNCTION_INITIALIZATION_ERROR;
import static java.util.Objects.requireNonNull;

public class HiveFunctionNamespaceManagerFactory
        implements FunctionNamespaceManagerFactory
{
    private static final Logger log = Logger.get(HiveFunctionNamespaceManagerFactory.class);

    private final ClassLoader classLoader;
    private final FunctionHandleResolver functionHandleResolver;

    public static final String NAME = "hive-functions";

    public HiveFunctionNamespaceManagerFactory(ClassLoader classLoader)
    {
        // Register hive-udfs
        URLClassLoader urlClassLoader = null;
        try {
            urlClassLoader = new URLClassLoader(new URL[]{
                    new File("/tmp/core-udfs-1.0-SNAPSHOT-standalone.jar").toURI().toURL()
            }, classLoader);
            // Thread.currentThread().setContextClassLoader(urlClassLoader);
            // registerUdfs(urlClassLoader);
        }
        catch (MalformedURLException e) {
            e.printStackTrace();
        }
        this.classLoader = requireNonNull(urlClassLoader, "classLoader is null");
        this.functionHandleResolver = new HiveFunctionHandleResolver();

        try {
            // the class is needed for UDF registration
            // this step can be ignored as long as the class can be found by the classloader
            // it is only to force an import of the class so that the compilation does not fail on "unused declared dependencies"
            Class<?> ignored = this.classLoader.loadClass(LlapSigner.class.getName());
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(HIVE_FUNCTION_INITIALIZATION_ERROR, e);
        }
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public FunctionHandleResolver getHandleResolver()
    {
        return functionHandleResolver;
    }

    @Override
    public FunctionNamespaceManager<?> create(String catalogName, Map<String, String> config, FunctionNamespaceManagerContext context)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new HiveFunctionModule(catalogName, classLoader, context.getTypeManager()));

            Injector injector = app
                    .noStrictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();
            return injector.getInstance(FunctionNamespaceManager.class);
        }
    }
}
