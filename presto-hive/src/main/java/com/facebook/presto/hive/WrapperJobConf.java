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
package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class WrapperJobConf
        extends JobConf
{
    private final Configuration config;

    public WrapperJobConf(Configuration config)
    {
        super();
        this.config = requireNonNull(config, "config is null");
    }

    public Configuration getConfig()
    {
        return config;
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator()
    {
        return config.iterator();
    }

    @Override
    public Class<?> getClassByName(String name)
            throws ClassNotFoundException
    {
        return config.getClassByName(name);
    }

    @Override
    public Class<?> getClassByNameOrNull(String name)
    {
        return config.getClassByNameOrNull(name);
    }

    @Override
    public Class<?> getClass(String name, Class<?> defaultValue)
    {
        return config.getClass(name, defaultValue);
    }

    @Override
    public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface)
    {
        return config.getClass(name, defaultValue, xface);
    }

    @Override
    public void setClassLoader(ClassLoader classLoader)
    {
        config.setClassLoader(classLoader);
    }

    @Override
    public ClassLoader getClassLoader()
    {
        return config.getClassLoader();
    }

    @Override
    public void set(String name, String value)
    {
        config.set(name, value);
    }

    @Override
    public String get(String name, String defaultValue)
    {
        return config.get(name, defaultValue);
    }

    @Override
    public String get(String name)
    {
        if (config == null) {
            return super.get(name);
        }
        return config.get(name);
    }

    @Override
    public void setBoolean(String name, boolean value)
    {
        config.setBoolean(name, value);
    }

    @Override
    public void setBooleanIfUnset(String name, boolean value)
    {
        config.setBooleanIfUnset(name, value);
    }

    @Override
    public boolean getBoolean(String name, boolean defaultValue)
    {
        return config.getBoolean(name, defaultValue);
    }

    @Override
    public void setInt(String name, int value)
    {
        config.setInt(name, value);
    }

    @Override
    public int getInt(String name, int defaultValue)
    {
        return config.getInt(name, defaultValue);
    }

    @Override
    public int[] getInts(String name)
    {
        return config.getInts(name);
    }

    @Override
    public void setDouble(String name, double value)
    {
        config.setDouble(name, value);
    }

    @Override
    public double getDouble(String name, double defaultValue)
    {
        return config.getDouble(name, defaultValue);
    }

    @Override
    public void setFloat(String name, float value)
    {
        config.setFloat(name, value);
    }

    @Override
    public float getFloat(String name, float defaultValue)
    {
        return config.getFloat(name, defaultValue);
    }

    @Override
    public void setLong(String name, long value)
    {
        config.setLong(name, value);
    }

    @Override
    public long getLong(String name, long defaultValue)
    {
        return config.getLong(name, defaultValue);
    }

    @Override
    public synchronized void unset(String name)
    {
        config.unset(name);
    }

    @Override
    public synchronized void setIfUnset(String name, String value)
    {
        config.setIfUnset(name, value);
    }

    @Override
    public void readFields(DataInput in)
            throws IOException
    {
        config.readFields(in);
    }

    @Override
    public void write(DataOutput out)
            throws IOException
    {
        config.write(out);
    }

    @Override
    public void clear()
    {
        super.clear();
        config.clear();
    }
}
