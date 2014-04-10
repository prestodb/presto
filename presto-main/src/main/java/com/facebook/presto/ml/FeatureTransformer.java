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
package com.facebook.presto.ml;

import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class FeatureTransformer
        implements Classifier, Regressor
{
    private final Optional<Classifier> classifier;
    private final Optional<Regressor> regressor;

    protected FeatureTransformer(Classifier classifier)
    {
        this.classifier = Optional.of(classifier);
        this.regressor = Optional.absent();
    }

    protected FeatureTransformer(Regressor regressor)
    {
        this.classifier = Optional.absent();
        this.regressor = Optional.of(regressor);
    }

    @Override
    public ModelType getType()
    {
        return getDelegate().getType();
    }

    protected Model getDelegate()
    {
        if (classifier.isPresent()) {
            return classifier.get();
        }
        if (regressor.isPresent()) {
            return regressor.get();
        }
        throw new IllegalStateException("no delegate found");
    }

    @Override
    public int classify(FeatureVector features)
    {
        checkArgument(classifier.isPresent(), "delegate is not a classifier");
        return classifier.get().classify(transform(features));
    }

    @Override
    public double regress(FeatureVector features)
    {
        checkArgument(regressor.isPresent(), "delegate is not a regressor");
        return regressor.get().regress(transform(features));
    }

    /**
     * Default implementation just transforms each feature vector, and trains the delegate.
     * Subclasses can override this method if they need to learn state during training.
     */
    @Override
    public void train(Dataset dataset)
    {
        List<FeatureVector> transformed = new ArrayList<>();
        for (FeatureVector features : dataset.getDatapoints()) {
            transformed.add(transform(features));
        }
        getDelegate().train(new Dataset(dataset.getLabels(), transformed));
    }

    protected abstract FeatureVector transform(FeatureVector features);
}
