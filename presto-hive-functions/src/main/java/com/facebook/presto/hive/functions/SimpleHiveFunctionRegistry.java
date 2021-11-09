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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import javax.inject.Inject;

public class SimpleHiveFunctionRegistry
        implements HiveFunctionRegistry
{
    private final ClassLoader classLoader;

    @Inject
    public SimpleHiveFunctionRegistry(@ForHiveFunction ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    @Override
    public Class<?> getClass(QualifiedObjectName name)
            throws ClassNotFoundException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return FunctionRegistry.getFunctionInfo(name.getObjectName()).getFunctionClass();
        }
        catch (SemanticException | NullPointerException e) {
            throw new ClassNotFoundException("Class of function " + name + " not found", e);
        }
    }
}
