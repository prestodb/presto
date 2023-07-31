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

#include <folly/init/Init.h>
#include <glog/logging.h>
#include <sys/wait.h>
#include <ucp/api/ucp.h> // @manual=fbsource//third-party/ucx:ucp
#include <chrono>

#include "velox/experimental/gpu/Common.h"

#define UCS_STATUS_OK(_expr)                                 \
  if (::ucs_status_t _status = (_expr); _status != UCS_OK) { \
    LOG(FATAL) << ucs_status_string(_status);                \
  }

#define POSIX_STATUS_OK(_expr)     \
  if ((_expr) == -1) {             \
    LOG(FATAL) << strerror(errno); \
  }

DEFINE_int64(msg_size, 400 << 10, "");
DEFINE_int32(num_msgs, 10, "");
DEFINE_int32(send_dev, 0, "");
DEFINE_int32(recv_dev, 1, "");

using namespace facebook::velox::gpu;

template <typename F>
ssize_t blockingIO(F op, int fd, void* buf, size_t nbyte) {
  auto* cbuf = static_cast<char*>(buf);
  while (nbyte > 0) {
    auto done = op(fd, cbuf, nbyte);
    if (done == -1) {
      return -1;
    }
    cbuf += done;
    nbyte -= done;
  }
  return 0;
}

CudaPtr<char[]> generateTestString(int64_t size) {
  std::string tmp;
  tmp.reserve(size);
  for (int64_t i = 0; i < size; ++i) {
    tmp += 'A' + (i % 26);
  }
  auto ans = allocateDeviceMemory<char>(size);
  CUDA_CHECK_FATAL(
      cudaMemcpy(ans.get(), tmp.data(), size, cudaMemcpyHostToDevice));
  return ans;
}

void epClose(ucp_worker_h worker, ucp_ep_h ep) {
  auto* req = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FLUSH);
  if (UCS_PTR_IS_PTR(req)) {
    ucs_status_t status;
    do {
      ucp_worker_progress(worker);
      status = ucp_request_check_status(req);
    } while (status == UCS_INPROGRESS);
    ucp_request_free(req);
  } else {
    UCS_STATUS_OK(UCS_PTR_STATUS(req));
  }
}

constexpr int kTestAmId = 0;

void requestWait(
    ucp_worker_h worker,
    const std::vector<ucs_status_ptr_t>& requests,
    int* pending) {
  for (auto& req : requests) {
    if (!req) {
      --*pending;
    } else if (UCS_PTR_IS_ERR(req)) {
      UCS_STATUS_OK(UCS_PTR_STATUS(req));
    }
  }
  while (*pending > 0) {
    ucp_worker_progress(worker);
  }
  for (auto& req : requests) {
    if (req) {
      UCS_STATUS_OK(ucp_request_check_status(req));
      ucp_request_free(req);
    }
  }
}

void runSender(ucp_worker_h worker, ucp_address_t* peerAddr) {
  ucp_ep_h ep;
  ucp_ep_params_t epParams;
  // CUDA tranpsorts does not support UCP_ERR_HANDLING_MODE_PEER.
  epParams.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
  epParams.address = peerAddr;
  UCS_STATUS_OK(ucp_ep_create(worker, &epParams, &ep));
  auto msg = generateTestString(FLAGS_msg_size);
  int pending = FLAGS_num_msgs;
  ucp_request_param_t reqParam;
  reqParam.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE |
      UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
  // IOV does not work for GPU memory.
  reqParam.datatype = ucp_dt_make_contig(1);
  reqParam.user_data = &pending;
  reqParam.cb.send = [](void* /*request*/, ucs_status_t, void* userData) {
    VLOG(1) << "Sent one message";
    --*static_cast<int*>(userData);
  };
  std::vector<ucs_status_ptr_t> requests(FLAGS_num_msgs);
  auto t0 = std::chrono::system_clock::now();
  for (int i = 0; i < FLAGS_num_msgs; ++i) {
    requests[i] = ucp_am_send_nbx(
        ep, kTestAmId, nullptr, 0, msg.get(), FLAGS_msg_size, &reqParam);
  }
  requestWait(worker, requests, &pending);
  auto dt = std::chrono::system_clock::now() - t0;
  LOG(INFO) << "Sent all messages, throughput: "
            << FLAGS_msg_size * FLAGS_num_msgs / (dt.count() / 1e9) / (1 << 30)
            << " GiB/s";
  epClose(worker, ep);
}

void logData(void* data, size_t length) {
  constexpr int kLogLevel = 2;
  if (!VLOG_IS_ON(kLogLevel)) {
    return;
  }
  cudaPointerAttributes attrs;
  CUDA_CHECK_FATAL(cudaPointerGetAttributes(&attrs, data));
  VLOG(kLogLevel) << "cudaMemoryType: " << attrs.type;
  length = std::min<size_t>(length, 100);
  char* buf;
  if (attrs.type == cudaMemoryTypeDevice) {
    buf = static_cast<char*>(alloca(length));
    CUDA_CHECK_FATAL(cudaMemcpy(buf, data, length, cudaMemcpyDeviceToHost));
  } else {
    buf = static_cast<char*>(data);
  }
  VLOG(kLogLevel) << std::string_view(buf, length);
}

void runReceiver(ucp_worker_h worker) {
  auto buf = allocateDeviceMemory<char>(FLAGS_msg_size * FLAGS_num_msgs);
  std::vector<char*> msgs(FLAGS_num_msgs);
  for (int i = 0; i < FLAGS_num_msgs; ++i) {
    msgs[i] = buf.get() + i * FLAGS_msg_size;
  }
  struct ReceiveContext {
    int* pending;
    char* msg;
  };
  struct Context {
    ucp_worker_h worker;
    char** msgs;
    int pending;
    int rndvMsgCount;
    std::vector<ucs_status_ptr_t> recvRequests;
    ReceiveContext* recvCtxs;
  } ctx;
  ctx.worker = worker;
  ctx.msgs = msgs.data();
  ctx.pending = FLAGS_num_msgs;
  ctx.rndvMsgCount = 0;
  ctx.recvCtxs = static_cast<ReceiveContext*>(
      alloca(FLAGS_num_msgs * sizeof(ReceiveContext)));
  ucp_am_handler_param_t handlerParam;
  handlerParam.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
      UCP_AM_HANDLER_PARAM_FIELD_CB | UCP_AM_HANDLER_PARAM_FIELD_ARG;
  handlerParam.id = kTestAmId;
  handlerParam.arg = &ctx;
  handlerParam.cb = [](void* arg,
                       const void* /*header*/,
                       size_t /*header_length*/,
                       void* data,
                       size_t length,
                       const ucp_am_recv_param_t* param) {
    auto* ctx = static_cast<Context*>(arg);
    if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) {
      VLOG(1) << "Received rendezvous request, message size: " << length;
      if (length > FLAGS_msg_size) {
        LOG(FATAL) << "Unexpected message size: " << length;
      }
      auto i = ctx->rndvMsgCount++;
      if (ctx->rndvMsgCount > FLAGS_num_msgs) {
        LOG(FATAL) << "Unexpected number of messages";
      }
      char* msg = ctx->msgs[i];
      auto* recvCtx = ctx->recvCtxs + i;
      recvCtx->pending = &ctx->pending;
      recvCtx->msg = msg;
      ucp_request_param_t reqParam;
      reqParam.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE |
          UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA |
          UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
      reqParam.datatype = ucp_dt_make_contig(1);
      reqParam.user_data = recvCtx;
      reqParam.cb.recv_am =
          [](void* /*request*/, ucs_status_t, size_t length, void* userData) {
            VLOG(1) << "Received one rendezvous message, size: " << length;
            auto* recvCtx = static_cast<ReceiveContext*>(userData);
            --*recvCtx->pending;
            logData(recvCtx->msg, length);
          };
      auto* req =
          ucp_am_recv_data_nbx(ctx->worker, data, msg, length, &reqParam);
      if (!req) {
        LOG(FATAL)
            << "Receive operation completed immediately, check whether callback is invoked";
      } else if (UCS_PTR_IS_ERR(req)) {
        UCS_STATUS_OK(UCS_PTR_STATUS(req));
      } else {
        ctx->recvRequests.push_back(req);
      }
      return UCS_INPROGRESS;
    }
    // GPUDirect is not used for eager protocol.
    VLOG(1) << "Received eager message with " << length << " bytes";
    logData(data, length);
    --ctx->pending;
    return UCS_OK;
  };
  UCS_STATUS_OK(ucp_worker_set_am_recv_handler(worker, &handlerParam));
  while (ctx.pending > 0) {
    ucp_worker_progress(worker);
  }
  for (auto* req : ctx.recvRequests) {
    if (req) {
      UCS_STATUS_OK(ucp_request_check_status(req));
      ucp_request_free(req);
    }
  }
  LOG(INFO) << "Received all messages";
}

// Check if GPUDirect is used: UCX_TLS=sm,cuda_copy,cuda_ipc
int main(int argc, char** argv) {
  folly::Init follyInit(&argc, &argv);
  ucp_config_t* config;
  UCS_STATUS_OK(ucp_config_read(nullptr, nullptr, &config));
  ucp_params_t params;
  params.field_mask = UCP_PARAM_FIELD_FEATURES;
  params.features = UCP_FEATURE_AM;

  int pipefd[2];
  POSIX_STATUS_OK(pipe(pipefd));
  auto childPid = fork();
  POSIX_STATUS_OK(childPid);

  ucp_context_h context;
  UCS_STATUS_OK(ucp_init(&params, config, &context));
  ucp_config_release(config);
  ucp_worker_params_t workerParams;
  workerParams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
  workerParams.thread_mode = UCS_THREAD_MODE_SINGLE;
  ucp_worker_h worker;
  UCS_STATUS_OK(ucp_worker_create(context, &workerParams, &worker));
  if (childPid == 0) {
    LOG(INFO) << "Sender created";
    CUDA_CHECK_FATAL(cudaSetDevice(FLAGS_send_dev));
    POSIX_STATUS_OK(close(pipefd[1]));
    size_t peerAddrLen;
    POSIX_STATUS_OK(
        blockingIO(read, pipefd[0], &peerAddrLen, sizeof(peerAddrLen)));
    auto* peerAddr = static_cast<ucp_address_t*>(alloca(peerAddrLen));
    POSIX_STATUS_OK(blockingIO(read, pipefd[0], peerAddr, peerAddrLen));
    POSIX_STATUS_OK(close(pipefd[0]));
    runSender(worker, peerAddr);
  } else {
    LOG(INFO) << "Receiver created";
    CUDA_CHECK_FATAL(cudaSetDevice(FLAGS_recv_dev));
    POSIX_STATUS_OK(close(pipefd[0]));
    ucp_address_t* localAddr;
    size_t localAddrLen;
    UCS_STATUS_OK(ucp_worker_get_address(worker, &localAddr, &localAddrLen));
    POSIX_STATUS_OK(
        blockingIO(write, pipefd[1], &localAddrLen, sizeof(localAddrLen)));
    POSIX_STATUS_OK(blockingIO(write, pipefd[1], localAddr, localAddrLen));
    POSIX_STATUS_OK(close(pipefd[1]));
    runReceiver(worker);
    wait(nullptr);
    ucp_worker_release_address(worker, localAddr);
  }
  ucp_worker_destroy(worker);
  ucp_cleanup(context);
  return 0;
}
