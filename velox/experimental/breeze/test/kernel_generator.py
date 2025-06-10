#!/usr/bin/env python3
# @nolint
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

# Copyright (c) 2024 by Rivos Inc.
# Licensed under the Apache License, Version 2.0, see LICENSE for details.
# SPDX-License-Identifier: Apache-2.0

from generator_common import (
    args,
    clang_format,
    extract_node,
    parse,
    read_extent,
    to_snake_case,
    CursorKind,
    COPYRIGHT,
)

import io
import subprocess
from abc import ABC, abstractmethod


AUTOGEN_HEADER = """/*
 * This file is auto-generated from kernel_generator.py
 * DO NOT EDIT!
 */
"""


def kernels_extractor(tu):
    kernels = []
    for c in tu.cursor.get_children():
        if c.displayname == "kernels" and c.kind == CursorKind.NAMESPACE:
            kernels = list(map(extract_node, c.get_children()))
    return kernels


def includes_extractor(tu):
    return list(
        filter(
            lambda x: x.kind == CursorKind.INCLUSION_DIRECTIVE, tu.cursor.get_children()
        )
    )


class KernelCodeGen(ABC):
    @property
    @abstractmethod
    def includes(self): ...

    @property
    @abstractmethod
    def platform_type(self): ...

    def num_warp_threads(self):
        return None

    @property
    def use_namespace(self):
        return True

    def get_templates_postfix(self, attrs):
        return []

    def write_func_attrs(self, out, attrs):
        pass

    def get_func_name(self, name):
        return name

    def get_args_prefix(self, attrs):
        return []

    def get_args(self, fn_props):
        args = list(map(lambda x: read_extent(x.extent), fn_props["function_params"]))
        return args

    def write_func_prologue(self, out, attrs):
        pass

    def generate(self, tu, filename):
        out = io.StringIO()
        # Copyright, autogen header, and platform specific includes
        out.write(f"{COPYRIGHT}\n{AUTOGEN_HEADER}\n{self.includes}\n")
        # Add general includes
        for i in includes_extractor(tu):
            out.write(read_extent(i.extent))
            out.write("\n")
        out.write("\n")
        # parse out the kernels and rewrite them in platform's style
        kernels = kernels_extractor(tu)
        if self.use_namespace:
            out.write("namespace kernels {\n\n")
        warp_threads = self.num_warp_threads()
        if warp_threads is not None:
            out.write(f"enum {{ WARP_THREADS = {warp_threads} }};")
        for kernel in kernels:
            kernel_name = kernel["spelling"]
            fn_props = kernel["fn_props"]
            template_params = fn_props["template_params"]
            attrs = fn_props["attrs"]
            body = fn_props["body"]

            # Consistency checks
            assert (kernel["kind"] == CursorKind.FUNCTION_TEMPLATE) == (
                len(template_params) != 0
            )

            # Add template parameters if applicable
            extra_template_args = self.get_templates_postfix(attrs)
            attrs["has_template_params"] = False
            if len(template_params) != 0 or len(extra_template_args) != 0:
                attrs["has_template_params"] = True
                out.write("template <")
                out.write(
                    ", ".join(
                        list(map(lambda x: read_extent(x.extent), template_params))
                        + extra_template_args
                    )
                )
                out.write(">\n")

            # Emit any function attributes needed
            self.write_func_attrs(out, attrs)

            # Write out function return type, name and arguments
            out.write(fn_props["return_type"].spelling)
            out.write(" ")
            out.write(self.get_func_name(kernel_name))
            out.write("(")
            out.write(", ".join(self.get_args_prefix(attrs) + self.get_args(fn_props)))
            out.write(")\n")

            # Begin the body
            out.write("{\n")
            self.write_func_prologue(out, attrs)
            out.write(read_extent(body.extent)[1:-1])
            out.write("}\n\n")
        if self.use_namespace:
            out.write("}  // namespace kernels\n")

        # Write out to specified file and clang-format the result
        with open(filename, "w") as outf:
            outf.write(out.getvalue())
        subprocess.run([clang_format, "-i", filename])


class OpenclBackend(KernelCodeGen):
    @property
    def includes(self):
        return '#include "breeze/platforms/opencl.h"\n'

    @property
    def platform_type(self):
        return "OpenCLPlatform<BLOCK_THREADS, WARP_THREADS>"

    @property
    def use_namespace(self):
        return False

    def num_warp_threads(self):
        return 32

    def write_func_attrs(self, out, attrs):
        # If this function isn't templated we may emit it as a kernel directly.
        if not attrs.get("has_template_params"):
            out.write("kernel ")

    def get_func_name(self, name):
        return to_snake_case(name)

    def get_templates_postfix(self, attrs):
        shared_mems = attrs.get("SharedMem", [])
        assert len(shared_mems) <= 1
        if shared_mems:
            return ["typename SharedMemType"]
        else:
            return []

    def write_func_prologue(self, out, attrs):
        platform_names = attrs.get("PlatformName", [])
        assert len(platform_names) <= 1
        for p in platform_names:
            out.write(f"  using PlatformT = {self.platform_type};")
            out.write(f"  PlatformT {p};")

    def get_args(self, fn_props):
        args = super().get_args(fn_props)

        # All pointers should be made annotated global
        args = list(map(lambda x: "global " + x if "*" in x else x, args))

        # shared_mem always precedes num_items (if it exists).
        shared_mems = fn_props["attrs"].get("SharedMem", [])
        assert len(shared_mems) <= 1
        if shared_mems:
            (_type, ident) = shared_mems[0].split(";", 1)
            shmem_arg = f"local SharedMemType* {ident}"
            if args[-1] == "int num_items":
                args = args[:-1] + [shmem_arg, args[-1]]
            else:
                args.append(shmem_arg)

        return args


class MetalBackend(KernelCodeGen):
    @property
    def includes(self):
        return '#include <metal_stdlib>\n#include "breeze/platforms/metal.h"\n'

    @property
    def platform_type(self):
        return "MetalPlatform<BLOCK_THREADS, WARP_THREADS>"

    @property
    def use_namespace(self):
        return False

    def num_warp_threads(self):
        return 32

    def write_func_attrs(self, out, attrs):
        # If this function isn't templated we may emit it as a kernel directly.
        if not attrs.get("has_template_params"):
            out.write("kernel ")

    def get_func_name(self, name):
        return to_snake_case(name)

    def get_templates_postfix(self, attrs):
        platform_names = attrs.get("PlatformName", [])
        shared_mems = attrs.get("SharedMem", [])
        assert len(platform_names) <= 1
        assert len(shared_mems) <= 1
        postfix = []
        if platform_names:
            postfix += ["typename PlatformT"]
        if shared_mems:
            postfix += ["typename SharedMemType"]
        return postfix

    def get_args_prefix(self, attrs):
        platform_names = attrs.get("PlatformName", [])
        assert len(platform_names) <= 1
        prefix = []
        if platform_names:
            prefix += [f"PlatformT {platform_names[0]}"]
        return prefix

    def get_args(self, fn_props):
        args = super().get_args(fn_props)

        # All pointers should be made annotated device
        args = list(map(lambda x: "device " + x if "*" in x else x, args))

        # shared_mem always precedes num_items (if it exists).
        shared_mems = fn_props["attrs"].get("SharedMem", [])
        assert len(shared_mems) <= 1
        if shared_mems:
            (_type, ident) = shared_mems[0].split(";", 1)
            shmem_arg = f"threadgroup SharedMemType* {ident}"
            if args[-1] == "int num_items":
                args = args[:-1] + [shmem_arg, args[-1]]
            else:
                args.append(shmem_arg)

        return args


class OpenmpBackend(KernelCodeGen):
    @property
    def includes(self):
        return '#include "breeze/platforms/openmp.h"\n'

    @property
    def platform_type(self):
        return "OpenMPPlatform<BLOCK_THREADS, BLOCK_THREADS>"

    def get_templates_postfix(self, attrs):
        platform_names = attrs.get("PlatformName", [])
        shared_mems = attrs.get("SharedMem", [])
        assert len(platform_names) <= 1
        assert len(shared_mems) <= 1
        postfix = []
        if shared_mems:
            postfix += ["typename SharedMemType"]
        if platform_names:
            postfix += [f"typename PlatformT = {self.platform_type}"]
        return postfix

    def get_args_prefix(self, attrs):
        platform_names = attrs.get("PlatformName", [])
        shared_mems = attrs.get("SharedMem", [])
        assert len(platform_names) <= 1
        assert len(shared_mems) <= 1
        prefix = []
        if platform_names:
            prefix += [f"PlatformT {platform_names[0]}"]
        if shared_mems:
            (_type, ident) = shared_mems[0].split(";", 1)
            prefix += [f"SharedMemType* {ident}"]
        return prefix


class SyclBackend(KernelCodeGen):
    @property
    def includes(self):
        return '#include "breeze/platforms/sycl.hpp"\n'

    @property
    def platform_type(self):
        return "SyCLPlatform<BLOCK_THREADS, WARP_THREADS>"

    def num_warp_threads(self):
        return 32

    def get_templates_postfix(self, attrs):
        shared_mems = attrs.get("SharedMem", [])
        assert len(shared_mems) <= 1
        if shared_mems:
            return ["typename SharedMemType"]
        else:
            return []

    def get_args_prefix(self, attrs):
        platform_names = attrs.get("PlatformName", [])
        assert len(platform_names) <= 1
        return ["cl::sycl::nd_item<1> item"]

    def get_args(self, fn_props):
        args = super().get_args(fn_props)

        # shared_mem always precedes num_items (if it exists).
        shared_mems = fn_props["attrs"].get("SharedMem", [])
        assert len(shared_mems) <= 1
        if shared_mems:
            (_type, ident) = shared_mems[0].split(";", 1)
            shmem_arg = f"SharedMemType* {ident}"
            if args[-1] == "int num_items":
                args = args[:-1] + [shmem_arg, args[-1]]
            else:
                args.append(shmem_arg)

        return args

    def write_func_prologue(self, out, attrs):
        platform_names = attrs.get("PlatformName", [])
        assert len(platform_names) <= 1
        for p in platform_names:
            out.write(f"  using PlatformT = {self.platform_type};")
            out.write(f"  PlatformT {p}{{item}};")


class CudaBackend(KernelCodeGen):
    @property
    def includes(self):
        return '#include "breeze/platforms/cuda.cuh"\n'

    @property
    def platform_type(self):
        return "CudaPlatform<BLOCK_THREADS, WARP_THREADS>"

    def num_warp_threads(self):
        return 32

    def write_func_attrs(self, out, attrs):
        # Add __global__ since this is a kernel
        out.write("__global__ ")

    def write_func_prologue(self, out, attrs):
        platform_names = attrs.get("PlatformName", [])
        assert len(platform_names) <= 1
        for p in platform_names:
            out.write(f"  using PlatformT = {self.platform_type};")
            out.write(f"  PlatformT {p};")
        shared_mems = attrs.get("SharedMem", [])
        for shared_mem in shared_mems:
            (type, ident) = shared_mem.split(";", 1)
            out.write(f"  __shared__ {type} {ident}_;\n")
            out.write(f"  auto {ident} = ({type}*)&{ident}_;\n")


class HipBackend(CudaBackend):
    @property
    def includes(self):
        return '#include "breeze/platforms/hip.hpp"\n'

    @property
    def platform_type(self):
        return "HipPlatform<BLOCK_THREADS, WARP_THREADS>"

    def num_warp_threads(self):
        return 32

    def write_func_attrs(self, out, attrs):
        # Add __global__ since this is a kernel
        out.write("__global__ ")


if __name__ == "__main__":
    index, tu = parse(args.template)
    # Normally it would be good to check tu.diagnostics here to ensure
    # there are no errors or other diagnostic messages outstanding,
    # thereby assuring that all the input is being parsed as expected.
    # However by nature of the function bodies being incomplete
    # (due to Platform and Shared symbols being factored out-of-line)
    # there will always be some error reported and so we disregard the
    # diagnostics and proceed optimistically.
    if args.backend == "cuda":
        CudaBackend().generate(tu, args.out)
    elif args.backend == "hip":
        HipBackend().generate(tu, args.out)
    elif args.backend == "sycl":
        SyclBackend().generate(tu, args.out)
    elif args.backend == "openmp":
        OpenmpBackend().generate(tu, args.out)
    elif args.backend == "opencl":
        OpenclBackend().generate(tu, args.out)
    elif args.backend == "metal":
        MetalBackend().generate(tu, args.out)
    else:
        raise ValueError(f"Unknown backend provided: {args.backend}")
