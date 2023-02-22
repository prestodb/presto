#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generate Documentation
# **NOTE**
# ********
# scripts assume there is a conda environment already
# created with the name as an argument to the script
# Example
#   Create Environment
#       `conda create -y --name pyveloxenv-docs python=3.7`
DOCS_CONDA_ENV=$1
ENVS=$(conda env list | grep $DOCS_CONDA_ENV)
if [ -z "$ENVS" ]
then
        echo "conda environment for documentation not available"
else
        echo "Installing doc generation dependencies..."
        source ${CONDA_PREFIX}/etc/profile.d/conda.sh
        conda activate ${DOCS_CONDA_ENV}
        conda install -y -c anaconda sphinx
        conda install -y -c conda-forge pandoc
        ## install Pyvelox
        LD_LIBRARY_PATH=/usr/local/lib make python-build
        ## generate the Python README
        cd velox/docs \
                && make clean \
                && pandoc ../../pyvelox/README.md --from markdown --to rst -s -o bindings/python/README_generated_pyvelox.rst \
                && LD_LIBRARY_PATH=/usr/local/lib make html
fi
