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

#include "metal_test.h"

@implementation MetalTest {
  id<MTLDevice> _mDevice;
  id<MTLLibrary> _mLibrary;
  id<MTLCommandQueue> _mCommandQueue;
}

- (instancetype)initWithDevice:(id<MTLDevice>)device {
  self = [super init];
  if (self) {
    _mDevice = device;

    NSError* error = nil;

    _mLibrary = [device newDefaultLibrary];
    if (_mLibrary == nil) {
      NSURL* url = [NSURL fileURLWithPath:@"" SHADER_LIB];
      _mLibrary = [device newLibraryWithURL:url error:&error];
      if (_mLibrary == nil) {
        NSLog(@"Failed to find the default library or " SHADER_LIB);
        return nil;
      }
    }

    _mCommandQueue = [device newCommandQueue];
    if (_mCommandQueue == nil) {
      NSLog(@"Failed to find the command queue.");
      return nil;
    }
  }
  return self;
}

- (id<MTLComputePipelineState>)createPipeline:(NSString*)functionWithName {
  id<MTLFunction> function = [_mLibrary newFunctionWithName:functionWithName];
  if (function == nil) {
    NSLog(@"Failed to find %@ function.", functionWithName);
  }
  assert(function != nil);
  NSError* error = nil;
  id<MTLComputePipelineState> pipeline = [_mDevice newComputePipelineStateWithFunction:function
                                                                                 error:&error];
  if (pipeline == nil) {
    NSLog(@"Failed to create pipeline for %@ function.", functionWithName);
  }
  assert(pipeline != nil);
  return pipeline;
}

- (id<MTLBuffer>)newBufferWithBytes:(const void*)pointer length:(NSUInteger)length {
  return [_mDevice newBufferWithBytes:pointer length:length options:MTLResourceStorageModeShared];
}

- (void)dispatchPipeline:(id<MTLComputePipelineState>)pipeline
               numBlocks:(int)numBlocks
         numBlockThreads:(int)numBlockThreads
                 buffers:(NSArray*)buffers {
  @autoreleasepool {
    id<MTLCommandBuffer> commandBuffer = [_mCommandQueue commandBuffer];
    id<MTLComputeCommandEncoder> encoder = [commandBuffer computeCommandEncoder];
    MTLSize gridSize = MTLSizeMake(numBlocks * numBlockThreads, 1, 1);
    MTLSize threadsPerThreadgroup = MTLSizeMake(numBlockThreads, 1, 1);
    for (NSUInteger i = 0; i < [buffers count]; ++i) {
      [encoder setBuffer:buffers[i] offset:0 atIndex:i];
    }
    [encoder setComputePipelineState:pipeline];
    [encoder dispatchThreads:gridSize threadsPerThreadgroup:threadsPerThreadgroup];
    [encoder endEncoding];
    [commandBuffer commit];
    [commandBuffer waitUntilCompleted];
  }
}

@end
