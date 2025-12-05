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

#include "presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestService.h"

#include <boost/beast/version.hpp>

#include "presto_cpp/main/functions/remote/tests/server/RestFunctionRegistry.h"
#include "presto_cpp/main/functions/remote/utils/ContentTypes.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"

using namespace facebook::velox;
namespace facebook::presto::functions::remote::rest::test {

RestSession::RestSession(boost::asio::ip::tcp::socket socket)
    : socket_(std::move(socket)),
      pool_(memory::memoryManager()->addLeafPool()) {}

void RestSession::run() {
  doRead();
}

void RestSession::doRead() {
  auto self = shared_from_this();
  boost::beast::http::async_read(
      socket_,
      buffer_,
      req_,
      [self](boost::beast::error_code ec, size_t bytes_transferred) {
        self->onRead(ec, bytes_transferred);
      });
}

void RestSession::onRead(
    boost::beast::error_code ec,
    std::size_t bytes_transferred) {
  boost::ignore_unused(bytes_transferred);

  if (ec == boost::beast::http::error::end_of_stream) {
    return doClose();
  }

  if (ec) {
    LOG(ERROR) << "Read error: " << ec.message();
    return;
  }

  handleRequest(std::move(req_));
}

void RestSession::handleRequest(
    const boost::beast::http::request<boost::beast::http::string_body>& req) {
  res_.version(req.version());
  res_.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);

  if (!ensurePostMethod(req)) {
    return;
  }

  if (!ensureValidContentType(req)) {
    return;
  }

  if (!ensureValidAcceptHeader(req)) {
    return;
  }

  std::string functionName;
  if (!extractFunctionName(req, functionName)) {
    return;
  }

  try {
    auto& registry = RestFunctionRegistry::getInstance();
    auto handler = registry.getFunction(functionName);
    if (!handler) {
      sendResponse(
          boost::beast::http::status::bad_request,
          plainText_,
          fmt::format("Function '{}' is not available.", functionName),
          false);
      return;
    }

    std::unique_ptr<VectorSerde> serde;
    if (contentType_ == remote::CONTENT_TYPE_PRESTO_PAGE) {
      serde = std::make_unique<serializer::presto::PrestoVectorSerde>();
    } else {
      serde = std::make_unique<serializer::spark::UnsafeRowVectorSerde>();
    }
    auto inputBuffer = folly::IOBuf::copyBuffer(req.body());

    std::string errorMessage;
    auto outputBuffer = handler->handleRequest(
        std::move(inputBuffer), serde.get(), pool_.get(), errorMessage);
    if (!errorMessage.empty()) {
      sendResponse(
          boost::beast::http::status::internal_server_error,
          plainText_,
          errorMessage,
          true);
      return;
    }
    sendSuccessResponse(std::move(outputBuffer));
  } catch (const std::exception& ex) {
    handleException(ex);
  }
}

void RestSession::sendResponse(
    boost::beast::http::status status,
    const std::string& contentType,
    const std::string& body,
    bool closeConnection) {
  res_.result(status);
  res_.set(boost::beast::http::field::content_type, contentType);
  res_.body() = body;
  res_.prepare_payload();

  auto self = shared_from_this();
  boost::beast::http::async_write(
      socket_,
      res_,
      [self, closeConnection](
          boost::beast::error_code ec, std::size_t bytes_transferred) {
        self->onWrite(closeConnection, ec, bytes_transferred);
      });
}

bool RestSession::ensurePostMethod(
    const boost::beast::http::request<boost::beast::http::string_body>& req) {
  if (req.method() == boost::beast::http::verb::post) {
    return true;
  }
  auto msg = fmt::format(
      "Only POST method is allowed. Method used: {}",
      std::string(req.method_string()));
  sendResponse(
      boost::beast::http::status::method_not_allowed, plainText_, msg, true);
  return false;
}

bool RestSession::ensureValidContentType(
    const boost::beast::http::request<boost::beast::http::string_body>& req) {
  auto contentType = req[boost::beast::http::field::content_type];
  if (!contentType.empty() &&
      (contentType == remote::CONTENT_TYPE_SPARK_UNSAFE_ROW ||
       contentType == remote::CONTENT_TYPE_PRESTO_PAGE)) {
    contentType_ = contentType;
    return true;
  }

  auto msg = fmt::format(
      "Unsupported Content-Type: '{}'. Expecting '{}' or '{}'.",
      std::string(contentType),
      remote::CONTENT_TYPE_PRESTO_PAGE,
      remote::CONTENT_TYPE_SPARK_UNSAFE_ROW);
  sendResponse(
      boost::beast::http::status::unsupported_media_type,
      plainText_,
      msg,
      true);
  return false;
}

bool RestSession::ensureValidAcceptHeader(
    const boost::beast::http::request<boost::beast::http::string_body>& req) {
  auto acceptHeader = req[boost::beast::http::field::accept];
  if (!acceptHeader.empty() &&
      (acceptHeader == remote::CONTENT_TYPE_PRESTO_PAGE ||
       acceptHeader == remote::CONTENT_TYPE_SPARK_UNSAFE_ROW)) {
    return true;
  }

  auto msg = fmt::format(
      "Unsupported Accept header: '{}'. Expecting '{}' or '{}'.",
      std::string(acceptHeader),
      remote::CONTENT_TYPE_PRESTO_PAGE,
      remote::CONTENT_TYPE_SPARK_UNSAFE_ROW);
  sendResponse(
      boost::beast::http::status::not_acceptable, plainText_, msg, true);
  return false;
}

bool RestSession::extractFunctionName(
    const boost::beast::http::request<boost::beast::http::string_body>&
        requestBody,
    std::string& functionName) {
  std::string path = std::string(requestBody.target());
  std::vector<std::string> pathComponents;
  folly::split('/', path, pathComponents);

  if (pathComponents.size() != 2) {
    sendResponse(
        boost::beast::http::status::bad_request,
        plainText_,
        "Invalid request path",
        true);
    return false;
  }

  // pathComponents[1] contains the function name
  functionName = pathComponents[1];
  return true;
}

void RestSession::handleException(const std::exception& ex) {
  sendResponse(
      boost::beast::http::status::internal_server_error,
      plainText_,
      ex.what(),
      true);
}

void RestSession::sendSuccessResponse(folly::IOBuf&& payload) {
  sendResponse(
      boost::beast::http::status::ok,
      contentType_,
      payload.moveToFbString().toStdString(),
      false);
}

void RestSession::onWrite(
    bool close,
    boost::beast::error_code ec,
    std::size_t bytes_transferred) {
  boost::ignore_unused(bytes_transferred);

  if (ec) {
    LOG(ERROR) << "Write error: " << ec.message();
    return;
  }

  if (close) {
    return doClose();
  }

  req_ = {};

  doRead();
}

void RestSession::doClose() {
  boost::beast::error_code ec;
  socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
}

RestListener::RestListener(
    boost::asio::io_context& ioc,
    boost::asio::ip::tcp::endpoint endpoint)
    : ioc_(ioc), acceptor_(ioc) {
  boost::beast::error_code ec;

  acceptor_.open(endpoint.protocol(), ec);
  if (ec) {
    LOG(ERROR) << "Open error: " << ec.message();
    return;
  }

  acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
  if (ec) {
    LOG(ERROR) << "Set_option error: " << ec.message();
    return;
  }

  acceptor_.bind(endpoint, ec);
  if (ec) {
    LOG(ERROR) << "Bind error: " << ec.message();
    return;
  }

  acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
  if (ec) {
    LOG(ERROR) << "Listen error: " << ec.message();
    return;
  }
}

void RestListener::run() {
  doAccept();
}

void RestListener::doAccept() {
  acceptor_.async_accept(
      [self = shared_from_this()](
          boost::beast::error_code ec, boost::asio::ip::tcp::socket socket) {
        self->onAccept(ec, std::move(socket));
      });
}

void RestListener::onAccept(
    boost::beast::error_code ec,
    boost::asio::ip::tcp::socket socket) {
  if (ec) {
    LOG(ERROR) << "Accept error: " << ec.message();
  } else {
    std::make_shared<RestSession>(std::move(socket))->run();
  }
  doAccept();
}

} // namespace facebook::presto::functions::remote::rest::test
