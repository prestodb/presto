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

#pragma once

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include "presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestHandler.h"
#include "presto_cpp/main/functions/remote/tests/server/RestFunctionRegistry.h"

namespace facebook::presto::functions::remote::rest::test {

/// @brief Manages an individual HTTP session.
/// Handles reading HTTP requests, processing them, and sending responses.
/// Inspired by the reference implementation described in:
/// https://medium.com/@AlexanderObregon/building-restful-apis-with-c-4c8ac63fe8a7
class RestSession : public std::enable_shared_from_this<RestSession> {
 public:
  RestSession(boost::asio::ip::tcp::socket socket);

  /// Starts the session by initiating a read operation.
  void run();

 private:
  // Initiates an asynchronous read operation.
  void doRead();

  // Called when a read operation completes.
  void onRead(boost::beast::error_code ec, std::size_t bytes_transferred);

  // Processes the HTTP request and prepares a response.
  void handleRequest(
      const boost::beast::http::request<boost::beast::http::string_body>& req);

  // Called when a write operation completes.
  void onWrite(
      bool close,
      boost::beast::error_code ec,
      std::size_t bytes_transferred);

  // Closes the socket connection.
  void doClose();

  // Helper to handle exceptions: logs error and sends HTTP 500
  void handleException(const std::exception& ex);

  // Helper to extract function name from /{functionName}
  bool extractFunctionName(
      const boost::beast::http::request<boost::beast::http::string_body>&
          requestBody,
      std::string& functionName);

  // Sends a response with status, contentType, and body. Then calls
  // async_write.
  void sendResponse(
      boost::beast::http::status status,
      const std::string& contentType,
      const std::string& body,
      bool closeConnection);

  // Helper to ensure the HTTP method is POST, else sends an error response.
  bool ensurePostMethod(
      const boost::beast::http::request<boost::beast::http::string_body>& req);

  // Helper to ensure Content-Type is either "application/X-presto-pages" or
  // "application/X-spark-unsafe-row". Otherwise sends an error response.
  bool ensureValidContentType(
      const boost::beast::http::request<boost::beast::http::string_body>& req);

  // Helper to ensure Accept is either "application/X-presto-pages" or
  // "application/X-spark-unsafe-row". Otherwise sends an error response.
  bool ensureValidAcceptHeader(
      const boost::beast::http::request<boost::beast::http::string_body>& req);

  // Sends a success response with the given payload.
  void sendSuccessResponse(folly::IOBuf&& payload);

  boost::asio::ip::tcp::socket socket_;
  boost::beast::flat_buffer buffer_;
  boost::beast::http::request<boost::beast::http::string_body> req_;
  boost::beast::http::response<boost::beast::http::string_body> res_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::string contentType_;
  static constexpr std::string plainText_ = "text/plain";
};

/// @brief Listens for incoming TCP connections and creates sessions.
/// Sets up a TCP acceptor to listen for client connections,
/// creating a new session for each accepted connection.
class RestListener : public std::enable_shared_from_this<RestListener> {
 public:
  RestListener(
      boost::asio::io_context& ioc,
      boost::asio::ip::tcp::endpoint endpoint);

  /// Starts accepting incoming connections.
  void run();

 private:
  // Initiates an asynchronous accept operation.
  void doAccept();

  // Called when an accept operation completes.
  void onAccept(
      boost::beast::error_code ec,
      boost::asio::ip::tcp::socket socket);

  boost::asio::io_context& ioc_;
  boost::asio::ip::tcp::acceptor acceptor_;
};

} // namespace facebook::presto::functions::remote::rest::test
