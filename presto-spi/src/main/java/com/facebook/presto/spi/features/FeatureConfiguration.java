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
package com.facebook.presto.spi.features;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FeatureConfiguration
{
    private String featureId;
    private boolean enabled;
    private String featureClass;
    private boolean hotReloadable;
    private List<String> featureInstances;
    private String currentInstance;
    private String defaultInstance;

    @JsonCreator
    public FeatureConfiguration(
            @JsonProperty String featureId,
            @JsonProperty boolean enabled,
            @JsonProperty boolean hotReloadable,
            @JsonProperty String featureClass,
            @JsonProperty List<String> featureInstances,
            @JsonProperty String currentInstance,
            @JsonProperty String defaultInstance)
    {
        this.featureId = featureId;
        this.featureClass = featureClass;
        this.enabled = enabled;
        this.hotReloadable = hotReloadable;
        this.featureInstances = featureInstances;
        this.currentInstance = currentInstance;
        this.defaultInstance = defaultInstance;
    }

    public static FeatureConfigurationBuilder builder()
    {
        return new FeatureConfigurationBuilder();
    }

    @JsonProperty
    public String getFeatureId()
    {
        return featureId;
    }

    public void setFeatureId(String featureId)
    {
        this.featureId = featureId;
    }

    @JsonProperty
    public boolean isEnabled()
    {
        return enabled;
    }

    public void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }

    @JsonProperty
    public String getFeatureClass()
    {
        return featureClass;
    }

    public void setFeatureClass(String featureClass)
    {
        this.featureClass = featureClass;
    }

    @JsonProperty
    public List<String> getFeatureInstances()
    {
        return featureInstances;
    }

    public void setFeatureInstances(List<String> featureInstances)
    {
        this.featureInstances = featureInstances;
    }

    @JsonProperty
    public String getCurrentInstance()
    {
        return currentInstance;
    }

    public void setCurrentInstance(String currentInstance)
    {
        this.currentInstance = currentInstance;
    }

    @JsonProperty
    public String getDefaultInstance()
    {
        return defaultInstance;
    }

    public void setDefaultInstance(String defaultInstance)
    {
        this.defaultInstance = defaultInstance;
    }

    public boolean getHotReloadable()
    {
        return hotReloadable;
    }

    public void setHotReloadable(boolean hotReloadable)
    {
        this.hotReloadable = hotReloadable;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FeatureConfiguration that = (FeatureConfiguration) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (!Objects.equals(featureId, that.featureId)) {
            return false;
        }
        if (!Objects.equals(featureClass, that.featureClass)) {
            return false;
        }
        if (!Objects.equals(featureInstances, that.featureInstances)) {
            return false;
        }
        return Objects.equals(currentInstance, that.currentInstance);
    }

    @Override
    public int hashCode()
    {
        int result = featureId != null ? featureId.hashCode() : 0;
        result = 31 * result + (enabled ? 1 : 0);
        result = 31 * result + (featureClass != null ? featureClass.hashCode() : 0);
        result = 31 * result + (featureInstances != null ? featureInstances.hashCode() : 0);
        result = 31 * result + (currentInstance != null ? currentInstance.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "FeatureConfiguration{" +
                "featureId='" + featureId + '\'' +
                ", enabled=" + enabled +
                ", featureClass='" + featureClass + '\'' +
                ", featureInstances=" + featureInstances +
                ", currentInstance='" + currentInstance + '\'' +
                '}';
    }

    public static class FeatureConfigurationBuilder
    {
        private String featureId;
        private boolean enabled = true;
        private String featureClass;
        private boolean hotReloadable;
        private List<String> featureInstances = new ArrayList<>();
        private String currentInstance;
        private String defaultInstance;

        FeatureConfigurationBuilder()
        {
        }

        public FeatureConfigurationBuilder featureId(String featureId)
        {
            this.featureId = featureId;
            return this;
        }

        public FeatureConfigurationBuilder enabled(boolean enabled)
        {
            this.enabled = enabled;
            return this;
        }

        public FeatureConfigurationBuilder featureClass(Class<?> featureClass)
        {
            this.featureClass = featureClass == null ? null : featureClass.getName();
            return this;
        }

        public FeatureConfigurationBuilder hotReloadable(boolean hotReloadable)
        {
            this.hotReloadable = hotReloadable;
            return this;
        }

        public FeatureConfigurationBuilder featureInstances(List<Class<?>> featureInstances)
        {
            this.featureInstances = featureInstances.stream().map(Class::getName).collect(Collectors.toList());
            return this;
        }

        public FeatureConfigurationBuilder currentInstance(Class<?> currentInstance)
        {
            this.currentInstance = currentInstance == null ? null : currentInstance.getName();
            return this;
        }

        public FeatureConfigurationBuilder defaultInstance(Class<?> defaultInstance)
        {
            this.defaultInstance = defaultInstance == null ? null : defaultInstance.getName();
            return this;
        }

        public FeatureConfiguration build()
        {
            return new FeatureConfiguration(featureId, enabled, hotReloadable, featureClass, featureInstances, currentInstance, defaultInstance);
        }

        public String toString()
        {
            return "FeatureConfiguration.FeatureConfigurationBuilder(featureId=" + this.featureId + ", enabled=" + this.enabled + ", featureClass=" + this.featureClass + ", hotReloadable=" + this.hotReloadable + ", featureInstances=" + this.featureInstances + ", currentInstance=" + this.currentInstance + ", defaultInstance=" + this.defaultInstance + ")";
        }
    }
}
