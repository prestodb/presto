/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>

#include <type_traits>
#include <vector>

@interface MetalTest : NSObject
- (instancetype)initWithDevice:(id<MTLDevice>)device;
- (id<MTLComputePipelineState>)createPipeline:(NSString*)functionWithName;
- (id<MTLBuffer>)newBufferWithBytes:(const void*)pointer length:(NSUInteger)length;
- (void)dispatchPipeline:(id<MTLComputePipelineState>)pipeline
               numBlocks:(int)numBlocks
         numBlockThreads:(int)numBlockThreads
                 buffers:(NSArray*)buffers;
@end

template <typename T>
struct is_std_vector : std::false_type {};
template <typename T>
struct is_std_vector<std::vector<T>> : std::true_type {};

template <typename T>
void* makeBuffer(MetalTest* test, const std::vector<T>& data) {
  return [test newBufferWithBytes:data.data() length:data.size() * sizeof(T)];
}

template <typename T, typename = std::enable_if_t<std::is_arithmetic_v<T>, void>>
void* makeBuffer(MetalTest* test, T data) {
  return [test newBufferWithBytes:&data length:sizeof(data)];
}

template <int BLOCK_THREADS, typename... Args>
void MetalTestDispatch(int numBlocks, MetalTest* test, NSString* pipelineName, Args&&... args) {
  @autoreleasepool {
    [&](auto... mtl_buffers) {
      auto pipeline = [test createPipeline:pipelineName];
      NSArray* buffers = @[ mtl_buffers... ];
      [test dispatchPipeline:pipeline
                   numBlocks:numBlocks
             numBlockThreads:BLOCK_THREADS
                     buffers:buffers];

      (([&] {
         if constexpr (is_std_vector<std::remove_reference_t<Args>>::value) {
           memcpy(args.data(), mtl_buffers.contents, args.size() * sizeof(args[0]));
         }
       })(),
       ...);
    }((id<MTLBuffer>)makeBuffer(test, args)...);
  }
}
