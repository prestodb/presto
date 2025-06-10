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
    FUNCTION_CURSOR_KINDS,
)

import io
import subprocess


AUTOGEN_HEADER = """/*
 * This file is auto-generated from test_fixture_generator.py
 * DO NOT EDIT!
 */
"""


def get_children_with_type(cursor, ty):
    return list(filter(lambda c: c.kind == ty, cursor.get_children()))


def get_child_with_type(cursor, ty):
    children = get_children_with_type(cursor, ty)
    assert len(children) == 1
    return children[0]


class TestFixtureGen:
    def includes(self, fixture_name):
        raise NotImplementedError

    def to_kernel_args(self, name, ty, kernel_args, preamble, postamble):
        if "std::vector" in ty:
            kernel_args.append(name)
        elif "*" in ty:
            # remove ptr from ty
            dty = ty[:-2]
            preamble.append(f"std::vector<{dty}> vec_{name}(1, *{name});")
            postamble.append(f"*{name} = vec_{name}[0];")
            kernel_args.append(f"vec_{name}")
        else:
            preamble.append(f"const std::vector<{ty}> vec_{name}(1, {name});")
            kernel_args.append(f"vec_{name}")

    def body(self, method, fixture_type_param, template_params, function_params):
        raise NotImplementedError

    def methods(self, test_fixture, fixture_type_param):
        methods = []
        for method in filter(
            lambda c: c.kind in FUNCTION_CURSOR_KINDS, test_fixture["children"]
        ):
            method = extract_node(method)

            template_params = list(
                map(extract_node, method["fn_props"]["template_params"])
            )
            function_params = list(
                map(extract_node, method["fn_props"]["function_params"])
            )

            templates = ", ".join(
                map(lambda node: read_extent(node["cursor"].extent), template_params)
            )
            templates = f"template <{templates}>" if len(templates) != 0 else ""
            name = method["spelling"]
            params = ", ".join(
                map(lambda node: read_extent(node["cursor"].extent), function_params)
            )
            body = self.body(
                method, fixture_type_param, template_params, function_params
            )

            methods.append(
                f"""
            {templates}
            void {name}({params}) {{
                {body}
            }}"""
            )

        return methods

    def class_preamble(self, fixture_name):
        return ""

    def generate(self, tu, filename):
        out = io.StringIO()

        test_fixture = extract_node(
            get_child_with_type(tu.cursor, CursorKind.CLASS_TEMPLATE)
        )
        assert (
            get_child_with_type(
                test_fixture["cursor"], CursorKind.CXX_BASE_SPECIFIER
            ).spelling
            == "::testing::Test"
        )

        fixture_name = test_fixture["spelling"]
        fixture_type_param = get_child_with_type(
            test_fixture["cursor"], CursorKind.TEMPLATE_TYPE_PARAMETER
        ).spelling
        methods = "\n".join(self.methods(test_fixture, fixture_type_param))

        out.write(
            f"""\
        {COPYRIGHT}

        {AUTOGEN_HEADER}

        #include <gtest/gtest.h>
        #include <vector>

        {self.includes(fixture_name)}

        template <typename {fixture_type_param}>
        class {fixture_name} : public ::testing::Test {{
         {self.class_preamble(fixture_name)}

         protected:
            {methods}
        }};
        """
        )

        with open(filename, "w") as outf:
            outf.write(out.getvalue())
        subprocess.run([clang_format, "-i", filename])


class HipTestFixture(TestFixtureGen):
    def __init__(self):
        self.launcher_fn = "HipTestLaunch"

    def includes(self, fixture_name):
        test_type = fixture_name.replace("Test", "").lower()
        return f"""
            #include <hip/hip_runtime.h>
            #include "test/generated/{test_type}s/kernels-hip.hpp"
            #include "test/platforms/hip_test.hpp"
        """

    def needs_shared_mem(self):
        return False

    def get_kernel_template_args(self, method, fixture_type_param, template_params):
        # Whether the kernel actually depends on the fixture type parameter
        untyped = method["fn_props"]["attrs"].get("untyped", False)
        if untyped:
            return list(map(lambda node: node["spelling"], template_params))

        templates = []
        inserted_fixture_type = False
        for t in template_params:
            # FIXME: The fixture template parameter (usually T) should always
            # be inserted before `U` or `H` (if its being used)
            if t["spelling"] in ["U", "H"]:
                templates.append(fixture_type_param)
                templates.append(t["spelling"])
                inserted_fixture_type = True
            else:
                # FIXME: No need to pass the template template parameter instead of just the
                # fully specified type in the tests.
                templates.append(
                    t["spelling"]
                    if not t["kind"] == CursorKind.TEMPLATE_TEMPLATE_PARAMETER
                    else t["spelling"] + f"<{fixture_type_param}>"
                )
        if not inserted_fixture_type:
            templates.append(fixture_type_param)

        return templates

    def body(self, method, fixture_type_param, template_params, function_params):
        preamble = []
        postamble = []
        maybe_add_shared_mem = ""

        if self.needs_shared_mem():
            shared_mem_type = method["fn_props"]["attrs"].get("shared_mem_type")
            if shared_mem_type:
                assert len(shared_mem_type) <= 1
                # Only OpenMP uses this code path and it requires that WARP_THREADS == BLOCK_THREADS
                shared_mem_type = shared_mem_type[0].replace(
                    "WARP_THREADS", "/*WARP_THREADS=*/BLOCK_THREADS"
                )
                platformt = ""
                if "PlatformT" in shared_mem_type:
                    platformt = f"using PlatformT = {self.platform_type}<BLOCK_THREADS, /*WARP_THREADS=*/BLOCK_THREADS>;"
                preamble += [
                    f"""
                    {platformt}
                    using SharedMemType = {shared_mem_type};"""
                ]
                maybe_add_shared_mem = ", SharedMemType"

        templates = ", ".join(
            self.get_kernel_template_args(method, fixture_type_param, template_params)
        )
        templates += maybe_add_shared_mem
        kernel_template_args = f"<{templates}>" if len(templates) != 0 else ""

        thread_count = (
            "BLOCK_THREADS"
            if "BLOCK_THREADS" in kernel_template_args
            else "/*BLOCK_THREADS=*/1"
        )
        block_count = "/*num_blocks=*/1"
        kernel_args = []
        last_arg = ""
        for p in function_params:
            name = p["spelling"]
            ty = p["type_spelling"]
            attrs = list(
                map(
                    lambda c: c.spelling,
                    get_children_with_type(p["cursor"], CursorKind.ANNOTATE_ATTR),
                )
            )
            assert len(attrs) <= 1
            if "use_as_size" in attrs:
                last_arg = name + ".size()"
            if "block_count" in attrs:
                block_count = name
                continue

            self.to_kernel_args(name, ty, kernel_args, preamble, postamble)

        if len(last_arg) != 0:
            kernel_args.append(last_arg)

        preamble = "".join(preamble)
        postamble = "".join(postamble)
        kernel_args = ", ".join([""] + kernel_args) if kernel_args else ""
        return f"""
            {preamble}
            {self.launcher_fn}<{thread_count}{maybe_add_shared_mem}>(
                {block_count},
                &kernels::{method["spelling"]}{kernel_template_args}{kernel_args}
            );
            {postamble}"""


class CudaTestFixture(HipTestFixture):
    def __init__(self):
        self.launcher_fn = "CudaTestLaunch"

    def includes(self, fixture_name):
        test_type = fixture_name.replace("Test", "").lower()
        return f"""
            #include "test/generated/{test_type}s/kernels-cuda.cuh"
            #include "breeze/platforms/cuda.cuh"
            #include "test/platforms/cuda_test.cuh"
        """


class OpenmpTestFixture(HipTestFixture):
    def __init__(self):
        self.launcher_fn = "OpenMPTestLaunch"
        self.platform_type = "OpenMPPlatform"

    def includes(self, fixture_name):
        test_type = fixture_name.replace("Test", "").lower()
        return f"""
            #include <omp.h>
            #include <cassert>

            #include "test/generated/{test_type}s/kernels-openmp.h"
            #include "breeze/platforms/openmp.h"
            #include "test/platforms/openmp_test.h"
        """

    def needs_shared_mem(self):
        return True

    def to_kernel_args(self, name, ty, kernel_args, preamble, postamble):
        if "std::vector" in ty:
            kernel_args.append(f"{name}.data()")
        elif "*" in ty:
            kernel_args.append(name)
        else:
            kernel_args.append(f"&{name}")


class SyclTestFixture(HipTestFixture):
    def __init__(self):
        self.launcher_fn = "SyclTestSubmit"
        self.platform_type = "SyCLPlatform"

    def includes(self, fixture_name):
        test_type = fixture_name.replace("Test", "").lower()
        return f"""
            #pragma GCC diagnostic push
            #pragma GCC diagnostic ignored "-Wunused-parameter"
            #pragma GCC diagnostic ignored "-Wunused-local-typedef"
            #pragma GCC diagnostic ignored "-Wunused-variable"
            #pragma GCC diagnostic ignored "-Wsign-compare"
            #pragma GCC diagnostic ignored "-Wmismatched-tags"
            #pragma GCC diagnostic ignored "-Wdeprecated-copy"
            #include <CL/sycl.hpp>
            #pragma GCC diagnostic pop

            #include "test/generated/{test_type}s/kernels-sycl.hpp"
            #include "test/platforms/sycl_test.hpp"

            using kernels::WARP_THREADS;
        """

    def needs_shared_mem(self):
        return True

    # Shares the same overall structure as HIP and OpenMP, but more work needs
    # to be done to first create a wrapping lambda to pass into SyclTestSubmit
    def body(self, method, fixture_type_param, template_params, function_params):
        preamble = []
        postamble = []
        maybe_add_shared_mem = ""
        last_inner_args = []
        last_lambda_params = []

        if self.needs_shared_mem():
            shared_mem_type = method["fn_props"]["attrs"].get("shared_mem_type")
            if shared_mem_type:
                assert len(shared_mem_type) <= 1
                shared_mem_type = shared_mem_type[0]
                platformt = ""
                if "PlatformT" in shared_mem_type:
                    platformt = f"using PlatformT = {self.platform_type}<BLOCK_THREADS, WARP_THREADS>;"
                preamble += [
                    f"""
                    {platformt}
                    using SharedMemType = {shared_mem_type};"""
                ]
                maybe_add_shared_mem = ", SharedMemType"
                last_inner_args += ["shared_mem"]
                last_lambda_params += ["SharedMemType* shared_mem"]

        templates = ", ".join(
            self.get_kernel_template_args(method, fixture_type_param, template_params)
        )
        templates += maybe_add_shared_mem
        kernel_template_args = f"<{templates}>" if len(templates) != 0 else ""

        thread_count = (
            "BLOCK_THREADS"
            if "BLOCK_THREADS" in kernel_template_args
            else "/*BLOCK_THREADS=*/1"
        )
        block_count = "/*num_blocks=*/1"
        inner_kernel_args = ["item"]
        kernel_args = []
        lambda_params = ["cl::sycl::nd_item<1> item"]
        size_arg = ""
        for p in function_params:
            name = p["spelling"]
            ty = p["type_spelling"]
            attrs = list(
                map(
                    lambda c: c.spelling,
                    get_children_with_type(p["cursor"], CursorKind.ANNOTATE_ATTR),
                )
            )
            assert len(attrs) <= 1
            if "use_as_size" in attrs:
                size_arg = "num_items = " + name + ".size()"
                last_inner_args += ["num_items"]
            if "block_count" in attrs:
                block_count = name
                continue

            self.to_kernel_args(name, ty, kernel_args, preamble, postamble)

            # All arguments to SyclTestSubmit are either already vectors or have
            # now been copied to a vector, and so for the respective lambda
            # params we can decay to the underlying pointer type which should
            # handle any const-ness as appropriate.
            lambda_params.append(f"decltype({kernel_args[-1]}.data()) {name}")
            inner_kernel_args += [name]

        inner_kernel_args += last_inner_args
        lambda_params += last_lambda_params

        preamble = "".join(preamble)
        postamble = "".join(postamble)
        kernel_args = ", ".join([""] + kernel_args) if kernel_args else ""
        inner_kernel_args = ", ".join(inner_kernel_args)
        lambda_params = ", ".join(lambda_params)
        lambda_fn = f"""\
            [{size_arg}]({lambda_params}){{
                kernels::{method["spelling"]}{kernel_template_args}({inner_kernel_args});
            }}\
        """
        return f"""
            {preamble}
            {self.launcher_fn}<{thread_count}{maybe_add_shared_mem}>(
                {block_count}, {lambda_fn}{kernel_args}
            );
            {postamble}"""


class OpenclTestFixture(TestFixtureGen):
    def __init__(self):
        self.launcher_fn = "OpenCLTestDispatch"

    def includes(self, fixture_name):
        return """
            #include <memory>

            #include "test/platforms/opencl_test.h"
            #include "test/type_helpers.h"
        """

    def class_preamble(self, fixture_name):
        return f"""
         private:
          struct Impl {{
            OpenCLTest test;
          }};
          std::unique_ptr<Impl> impl_;

         protected:
          {fixture_name}() : impl_(std::make_unique<Impl>()) {{}}
          ~{fixture_name}() override {{}}

          void SetUp() override {{}}
          void TearDown() override {{}}
        """

    def get_kernel_template_args(self, method, fixture_type_param, template_params):
        template_variants = []
        template_sizes = []

        for t in template_params:
            if t["kind"] == CursorKind.TEMPLATE_TYPE_PARAMETER:
                template_variants += [t["spelling"]]
            elif t["kind"] == CursorKind.TEMPLATE_TEMPLATE_PARAMETER:
                template_variants += [t["spelling"] + f"<{fixture_type_param}>"]
            elif t["kind"] == CursorKind.TEMPLATE_NON_TYPE_PARAMETER:
                template_sizes += [t["spelling"]]

            # FIXME: No need to pass the template template parameter instead of just the
            # fully specified type in the tests.

        # Whether the kernel actually depends on the fixture type parameter
        untyped = method["fn_props"]["attrs"].get("untyped", False)
        if not untyped:
            template_variants += [fixture_type_param]

        return (template_variants, template_sizes)

    def framework_specific_args(self):
        return "&impl_->test, kernel_name"

    def body(self, method, fixture_type_param, template_params, function_params):
        preamble = []
        postamble = []

        (template_variants, template_sizes) = self.get_kernel_template_args(
            method, fixture_type_param, template_params
        )
        op = "op"
        if template_variants:
            op = f"add_op_variant<{op}, {', '.join(template_variants)}>"
        if template_sizes:
            op = f"add_instance_shape<{op}, {', '.join(template_sizes)}>"

        thread_count = (
            "BLOCK_THREADS"
            if "BLOCK_THREADS" in template_sizes
            else "/*BLOCK_THREADS=*/1"
        )
        block_count = "/*numBlocks=*/1"
        kernel_args = []
        last_arg = ""
        for p in function_params:
            name = p["spelling"]
            ty = p["type_spelling"]
            attrs = list(
                map(
                    lambda c: c.spelling,
                    get_children_with_type(p["cursor"], CursorKind.ANNOTATE_ATTR),
                )
            )
            assert len(attrs) <= 1
            if "use_as_size" in attrs:
                last_arg = f"static_cast<int>({name}.size())"
            if "block_count" in attrs:
                block_count = name
                continue

            self.to_kernel_args(name, ty, kernel_args, preamble, postamble)

        if len(last_arg) != 0:
            kernel_args.append(last_arg)

        preamble += [
            f'constexpr static const char op[] = "{to_snake_case(method["spelling"])}";'
        ]
        preamble += [f"constexpr const char* kernel_name = {op};"]

        preamble = "".join(preamble)
        postamble = "".join(postamble)
        kernel_args = ", ".join([""] + kernel_args) if kernel_args else ""
        return f"""
            {preamble}
            {self.launcher_fn}<{thread_count}>(
                {block_count}, {self.framework_specific_args()}{kernel_args}
            );
            {postamble}"""


class MetalTestFixture(OpenclTestFixture):
    def __init__(self):
        self.launcher_fn = "MetalTestDispatch"

    def includes(self, fixture_name):
        return """
            #include <memory>

            #include "test/platforms/metal_test.h"
            #include "test/type_helpers.h"
        """

    def class_preamble(self, fixture_name):
        return f"""
         private:
          struct Impl {{
            MetalTest* test;
          }};
          std::unique_ptr<Impl> impl_;

         protected:
          {fixture_name}() : impl_(std::make_unique<Impl>()) {{}}
          ~{fixture_name}() override {{}}

          void SetUp() override {{
            impl_->test = [[MetalTest alloc] initWithDevice:MTLCreateSystemDefaultDevice()];
          }}
          void TearDown() override {{
            [impl_->test release];
          }}
        """

    def framework_specific_args(self):
        return "impl_->test, @(kernel_name)"


if __name__ == "__main__":
    index, tu = parse(args.template)

    if tu.diagnostics:
        print(f"\nParsing template {args.template} failed with errors:")
        for d in tu.diagnostics:
            print(f"    {repr(d)}")
        print()
        raise ValueError("Non-zero compiler diagnostics, chickening out")

    if args.backend == "cuda":
        CudaTestFixture().generate(tu, args.out)
    elif args.backend == "hip":
        HipTestFixture().generate(tu, args.out)
    elif args.backend == "openmp":
        OpenmpTestFixture().generate(tu, args.out)
    elif args.backend == "opencl":
        OpenclTestFixture().generate(tu, args.out)
    elif args.backend == "metal":
        MetalTestFixture().generate(tu, args.out)
    elif args.backend == "sycl":
        SyclTestFixture().generate(tu, args.out)
    else:
        raise ValueError(f"Unknown backend provided: {args.backend}")
