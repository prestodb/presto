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

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import io.airlift.concurrent.Threads;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractSvmModel
        implements Model
{
    protected svm_model model;

    protected AbstractSvmModel()
    {
    }

    protected AbstractSvmModel(svm_model model)
    {
        this.model = checkNotNull(model, "model is null");
    }

    @Override
    public byte[] getSerializedData()
    {
        File file = null;
        try {
            // libsvm doesn't have a method to serialize the model into a buffer, so write it out to a file and then read it back in
            file = File.createTempFile("svm", null);
            svm.svm_save_model(file.getAbsolutePath(), model);
            return Files.toByteArray(file);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if (file != null) {
                file.delete();
            }
        }
    }

    @Override
    public void train(Dataset dataset)
    {
        svm_parameter param = new svm_parameter();
        // default values
        param.svm_type = getLibsvmType();
        param.kernel_type = svm_parameter.LINEAR;
        param.degree = 3;
        param.gamma = 0;
        param.coef0 = 0;
        param.nu = 0.5;
        param.cache_size = 100;
        param.C = 1;
        param.eps = 0.1;
        param.p = 0.1;
        param.shrinking = 1;
        param.probability = 0;
        param.nr_weight = 0;
        param.weight_label = new int[0];
        param.weight = new double[0];

        svm_problem problem = toSvmProblem(dataset);

        ExecutorService service = Executors.newCachedThreadPool(Threads.threadsNamed("libsvm-trainer-" + System.identityHashCode(this) + "-%d"));
        try {
            TimeLimiter limiter = new SimpleTimeLimiter(service);
            //TODO: this time limit should be configurable
            model = limiter.callWithTimeout(getTrainingFunction(problem, param), 1, TimeUnit.HOURS, true);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
        finally {
            service.shutdownNow();
        }
    }

    private static Callable<svm_model> getTrainingFunction(final svm_problem problem, final svm_parameter param)
    {
        return new Callable<svm_model>() {
            @Override
            public svm_model call()
                    throws Exception
            {
                return svm.svm_train(problem, param);
            }
        };
    }

    protected abstract int getLibsvmType();

    private static svm_problem toSvmProblem(Dataset dataset)
    {
        svm_problem problem = new svm_problem();
        List<Double> labels = dataset.getLabels();
        problem.l = labels.size();
        problem.y = new double[labels.size()];
        for (int i = 0; i < labels.size(); i++) {
            problem.y[i] = labels.get(i);
        }
        problem.x = new svm_node[labels.size()][];
        for (int i = 0; i < dataset.getDatapoints().size(); i++) {
            problem.x[i] = toSvmNodes(dataset.getDatapoints().get(i));
        }
        return problem;
    }

    protected static svm_node[] toSvmNodes(FeatureVector features)
    {
        svm_node[] nodes = new svm_node[features.size()];
        int i = 0;
        // Features map is sorted, so we can just flatten it to a list for libsvm
        for (SortedMap.Entry<Integer, Double> feature : features.getFeatures().entrySet()) {
            nodes[i] = new svm_node();
            nodes[i].index = feature.getKey();
            nodes[i].value = feature.getValue();
            i++;
        }

        return nodes;
    }
}
