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

import setuptools

with open("README.md", "r") as readme:
    long_description = readme.read()


setuptools.setup(
    name="veloxbench",
    version="0.0.1",
    description="Velox Benchmarks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    entry_points={},
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache 2 License",
    ],
    python_requires=">=3.8",
    maintainer="Velox Developers",
    url="https://github.com/facebookincubator/velox",
)
