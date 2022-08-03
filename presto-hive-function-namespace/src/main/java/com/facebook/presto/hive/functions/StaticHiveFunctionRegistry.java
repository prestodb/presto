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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import javax.inject.Inject;

import java.net.URLClassLoader;
import java.util.Arrays;

import static com.facebook.presto.hive.functions.FunctionRegistry.getCurrentFunctionNames;
import static com.facebook.presto.hive.functions.FunctionRegistry.getFunctionInfo;
import static java.util.Objects.requireNonNull;

public class StaticHiveFunctionRegistry
        implements HiveFunctionRegistry
{
    private static final Logger log = Logger.get(StaticHiveFunctionRegistry.class);
    private final ClassLoader classLoader;

    @Inject
    public StaticHiveFunctionRegistry(@ForHiveFunction ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        // registerUdfs((URLClassLoader) classLoader);
    }

    @Override
    public Class<?> getClass(QualifiedObjectName name)
            throws ClassNotFoundException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            getCurrentFunctionNames().stream().forEach(log::info);
            log.info("Function name: " + name.getObjectName());
            Arrays.stream(((URLClassLoader) classLoader).getURLs()).forEach(url -> log.info(url.getFile()));
            return getFunctionInfo(name.getObjectName()).getFunctionClass();
        }
        catch (SemanticException | NullPointerException e) {
            e.printStackTrace();
            throw new ClassNotFoundException("Class of function " + name + " not found", e);
        }
    }

    public void registerUdfs(URLClassLoader urlClassLoader)
    {
        try {
            log.info("Registering hive-udfs static function registry");
            Arrays.stream((urlClassLoader).getURLs()).forEach(url -> log.info(url.getFile()));
            Class<?> loaderClass = Class.forName("RegisterUDFs", true, urlClassLoader);
            loaderClass.newInstance();
            Arrays.stream((urlClassLoader).getURLs()).forEach(url -> log.info(url.getFile()));
            log.info("Registered hive-udfs");
        }
        catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
