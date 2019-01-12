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
package io.prestosql.plugin.ml;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Dataset
{
    private final List<FeatureVector> datapoints;
    private final List<Double> labels;
    private final Map<Integer, String> labelEnumeration;

    public Dataset(List<Double> labels, List<FeatureVector> datapoints, Map<Integer, String> labelEnumeration)
    {
        requireNonNull(datapoints, "datapoints is null");
        requireNonNull(labels, "labels is null");
        requireNonNull(labelEnumeration, "labelEnumeration is null");
        checkArgument(datapoints.size() == labels.size(), "datapoints and labels have different sizes");
        this.labels = ImmutableList.copyOf(labels);
        this.datapoints = ImmutableList.copyOf(datapoints);
        this.labelEnumeration = ImmutableMap.copyOf(labelEnumeration);
    }

    public List<FeatureVector> getDatapoints()
    {
        return datapoints;
    }

    public List<Double> getLabels()
    {
        return labels;
    }

    public Map<Integer, String> getLabelEnumeration()
    {
        return labelEnumeration;
    }
}
