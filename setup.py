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

#!/usr/bin/env python
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import distutils.command.build
import distutils.command.clean
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from setuptools import Extension, find_packages, setup
from setuptools.command.build_ext import build_ext

ROOT_DIR = Path(__file__).parent.resolve()

with open("pyvelox/README.md") as f:
    readme = f.read()


# Override build directory
class BuildCommand(distutils.command.build.build):
    def initialize_options(self):
        distutils.command.build.build.initialize_options(self)
        self.build_base = "_build"


def _get_version():
    version = open("./version.txt").read().strip()
    version = re.sub("#.*\n?", "", version, flags=re.MULTILINE)
    sha = "Unknown"
    try:
        sha = (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(ROOT_DIR))
            .decode("ascii")
            .strip()
        )
    except Exception:
        pass

    if os.getenv("BUILD_VERSION"):
        version = os.getenv("BUILD_VERSION")
    elif sha != "Unknown":
        version += "+" + sha[:7]

    return version, sha


def _export_version(version, sha):
    version_path = ROOT_DIR / "pyvelox" / "version.py"
    with open(version_path, "w") as f:
        f.write("__version__ = '{}'\n".format(version))
        f.write("git_version = {}\n".format(repr(sha)))


VERSION, SHA = _get_version()
_export_version(VERSION, SHA)

print("-- Building version " + VERSION)


class clean(distutils.command.clean.clean):
    def run(self):
        # Run default behavior first
        distutils.command.clean.clean.run(self)

        # Remove pyvelox extension
        for path in (ROOT_DIR / "pyvelox").glob("**/*.so"):
            print(f"removing '{path}'")
            path.unlink()
        # Remove build directory
        build_dirs = [
            ROOT_DIR / "_build",
        ]
        for path in build_dirs:
            if path.exists():
                print(f"removing '{path}' (and everything under it)")
                shutil.rmtree(str(path), ignore_errors=True)


# Based off of
# https://github.com/pytorch/audio/blob/2c8aad97fc8d7647ee8b2df2de9312cce0355ef6/build_tools/setup_helpers/extension.py#L46
class CMakeBuild(build_ext):
    def run(self):
        try:
            subprocess.check_output(["cmake", "--version"])
        except OSError:
            raise RuntimeError("CMake is not available.")
        super().run()

    def build_extension(self, ext):
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        # Allow using a pre-built Velox library (for CI and development) e.g. 'VELOX_BUILD_DIR=_build/velox/debug'
        # The build in question must have been built with 'VELOX_BUILD_PYTHON_PACKAGE=ON' and the same python version.
        if "VELOX_BUILD_DIR" in os.environ:
            velox_dir = os.path.abspath(os.environ["VELOX_BUILD_DIR"])

            if not os.path.isdir(extdir):
                os.symlink(velox_dir, os.path.dirname(extdir), target_is_directory=True)

            print(f"Using pre-built Velox library from {velox_dir}")
            return

        # required for auto-detection of auxiliary "native" libs
        if not extdir.endswith(os.path.sep):
            extdir += os.path.sep

        if "DEBUG" in os.environ:
            cfg = "Debug" if os.environ["DEBUG"] == "1" else "Release"
        else:
            cfg = "Debug" if self.debug else "Release"

        exec_path = sys.executable

        cmake_args = [
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}",
            f"-DCMAKE_BUILD_TYPE={cfg}",
            f"-DCMAKE_INSTALL_PREFIX={extdir}",
            "-DCMAKE_VERBOSE_MAKEFILE=ON",
            "-DVELOX_BUILD_MINIMAL=ON",
            "-DVELOX_BUILD_PYTHON_PACKAGE=ON",
            f"-DPYTHON_EXECUTABLE={exec_path} ",
        ]
        build_args = []

        # Default to Ninja
        if "CMAKE_GENERATOR" not in os.environ:
            cmake_args += ["-GNinja"]

        # Set CMAKE_BUILD_PARALLEL_LEVEL to control the parallel build level
        # across all generators.
        if "CMAKE_BUILD_PARALLEL_LEVEL" not in os.environ:
            # self.parallel is a Python 3 only way to set parallel jobs by hand
            # using -j in the build_ext call, not supported by pip or PyPA-build.
            if hasattr(self, "parallel") and self.parallel:
                # CMake 3.12+ only.
                build_args += ["-j{}".format(self.parallel)]

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        subprocess.check_call(
            ["cmake", str(ROOT_DIR)] + cmake_args,
            cwd=self.build_temp,
        )
        subprocess.check_call(
            ["cmake", "--build", "."] + build_args, cwd=self.build_temp
        )


setup(
    name="pyvelox",
    version=VERSION,
    description="Python bindings and extensions for Velox",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/facebookincubator/velox",
    author="Meta",
    author_email="velox@fb.com",
    license="Apache License 2.0",
    install_requires=[
        "cffi",
        "typing",
        "tabulate",
        "typing-inspect",
        "pyarrow",
    ],
    extras_require={"tests": ["pyarrow"]},
    python_requires=">=3.7",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: C++",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    packages=find_packages() + find_packages(where="./test"),
    zip_safe=False,
    ext_modules=[Extension(name="pyvelox.pyvelox", sources=[])],
    cmdclass={"build_ext": CMakeBuild, "clean": clean, "build": BuildCommand},
)
