/*
 *
 * Copyright 2016 gRPC authors.
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
 *
 */

#include "test/cpp/end2end/test_service_impl.h"

#include <grpc/support/log.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server_context.h>
#include <gtest/gtest.h>

#include <string>
#include <thread>

#include "src/proto/grpc/testing/echo.grpc.pb.h"
#include "test/cpp/util/string_ref_helper.h"

using std::chrono::system_clock;

namespace grpc {
namespace testing {
namespace {

// When echo_deadline is requested, deadline seen in the ServerContext is set in
// the response in seconds.
void MaybeEchoDeadline(experimental::ServerContextBase* context,
                       const EchoRequest* request, EchoResponse* response) {
  if (request->has_param() && request->param().echo_deadline()) {
    gpr_timespec deadline = gpr_inf_future(GPR_CLOCK_REALTIME);
    if (context->deadline() != system_clock::time_point::max()) {
      Timepoint2Timespec(context->deadline(), &deadline);
    }
    response->mutable_param()->set_request_deadline(deadline.tv_sec);
  }
}

void CheckServerAuthContext(
    const experimental::ServerContextBase* context,
    const grpc::string& expected_transport_security_type,
    const grpc::string& expected_client_identity) {
  std::shared_ptr<const AuthContext> auth_ctx = context->auth_context();
  std::vector<grpc::string_ref> tst =
      auth_ctx->FindPropertyValues("transport_security_type");
  EXPECT_EQ(1u, tst.size());
  EXPECT_EQ(expected_transport_security_type, ToString(tst[0]));
  if (expected_client_identity.empty()) {
    EXPECT_TRUE(auth_ctx->GetPeerIdentityPropertyName().empty());
    EXPECT_TRUE(auth_ctx->GetPeerIdentity().empty());
    EXPECT_FALSE(auth_ctx->IsPeerAuthenticated());
  } else {
    auto identity = auth_ctx->GetPeerIdentity();
    EXPECT_TRUE(auth_ctx->IsPeerAuthenticated());
    EXPECT_EQ(1u, identity.size());
    EXPECT_EQ(expected_client_identity, identity[0]);
  }
}

// Returns the number of pairs in metadata that exactly match the given
// key-value pair. Returns -1 if the pair wasn't found.
int MetadataMatchCount(
    const std::multimap<grpc::string_ref, grpc::string_ref>& metadata,
    const grpc::string& key, const grpc::string& value) {
  int count = 0;
  for (const auto& metadatum : metadata) {
    if (ToString(metadatum.first) == key &&
        ToString(metadatum.second) == value) {
      count++;
    }
  }
  return count;
}
}  // namespace

namespace {
int GetIntValueFromMetadataHelper(
    const char* key,
    const std::multimap<grpc::string_ref, grpc::string_ref>& metadata,
    int default_value) {
  if (metadata.find(key) != metadata.end()) {
    std::istringstream iss(ToString(metadata.find(key)->second));
    iss >> default_value;
    gpr_log(GPR_INFO, "%s : %d", key, default_value);
  }

  return default_value;
}

int GetIntValueFromMetadata(
    const char* key,
    const std::multimap<grpc::string_ref, grpc::string_ref>& metadata,
    int default_value) {
  return GetIntValueFromMetadataHelper(key, metadata, default_value);
}

void ServerTryCancel(ServerContext* context) {
  EXPECT_FALSE(context->IsCancelled());
  context->TryCancel();
  gpr_log(GPR_INFO, "Server called TryCancel() to cancel the request");
  // Now wait until it's really canceled
  while (!context->IsCancelled()) {
    gpr_sleep_until(gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                 gpr_time_from_micros(1000, GPR_TIMESPAN)));
  }
}

void ServerTryCancelNonblocking(experimental::CallbackServerContext* context) {
  EXPECT_FALSE(context->IsCancelled());
  context->TryCancel();
  gpr_log(GPR_INFO, "Server called TryCancel() to cancel the request");
}

}  // namespace

Status TestServiceImpl::Echo(ServerContext* context, const EchoRequest* request,
                             EchoResponse* response) {
  if (request->has_param() &&
      request->param().server_notify_client_when_started()) {
    signaller_.SignalClientThatRpcStarted();
    signaller_.ServerWaitToContinue();
  }

  // A bit of sleep to make sure that short deadline tests fail
  if (request->has_param() && request->param().server_sleep_us() > 0) {
    gpr_sleep_until(
        gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                     gpr_time_from_micros(request->param().server_sleep_us(),
                                          GPR_TIMESPAN)));
  }

  if (request->has_param() && request->param().server_die()) {
    gpr_log(GPR_ERROR, "The request should not reach application handler.");
    GPR_ASSERT(0);
  }
  if (request->has_param() && request->param().has_expected_error()) {
    const auto& error = request->param().expected_error();
    return Status(static_cast<StatusCode>(error.code()), error.error_message(),
                  error.binary_error_details());
  }
  int server_try_cancel = GetIntValueFromMetadata(
      kServerTryCancelRequest, context->client_metadata(), DO_NOT_CANCEL);
  if (server_try_cancel > DO_NOT_CANCEL) {
    // Since this is a unary RPC, by the time this server handler is called,
    // the 'request' message is already read from the client. So the scenarios
    // in server_try_cancel don't make much sense. Just cancel the RPC as long
    // as server_try_cancel is not DO_NOT_CANCEL
    ServerTryCancel(context);
    return Status::CANCELLED;
  }

  response->set_message(request->message());
  MaybeEchoDeadline(context, request, response);
  if (host_) {
    response->mutable_param()->set_host(*host_);
  }
  if (request->has_param() && request->param().client_cancel_after_us()) {
    {
      std::unique_lock<std::mutex> lock(mu_);
      signal_client_ = true;
    }
    while (!context->IsCancelled()) {
      gpr_sleep_until(gpr_time_add(
          gpr_now(GPR_CLOCK_REALTIME),
          gpr_time_from_micros(request->param().client_cancel_after_us(),
                               GPR_TIMESPAN)));
    }
    return Status::CANCELLED;
  } else if (request->has_param() &&
             request->param().server_cancel_after_us()) {
    gpr_sleep_until(gpr_time_add(
        gpr_now(GPR_CLOCK_REALTIME),
        gpr_time_from_micros(request->param().server_cancel_after_us(),
                             GPR_TIMESPAN)));
    return Status::CANCELLED;
  } else if (!request->has_param() ||
             !request->param().skip_cancelled_check()) {
    EXPECT_FALSE(context->IsCancelled());
  }

  if (request->has_param() && request->param().echo_metadata_initially()) {
    const std::multimap<grpc::string_ref, grpc::string_ref>& client_metadata =
        context->client_metadata();
    for (const auto& metadatum : client_metadata) {
      context->AddInitialMetadata(ToString(metadatum.first),
                                  ToString(metadatum.second));
    }
  }

  if (request->has_param() && request->param().echo_metadata()) {
    const std::multimap<grpc::string_ref, grpc::string_ref>& client_metadata =
        context->client_metadata();
    for (const auto& metadatum : client_metadata) {
      context->AddTrailingMetadata(ToString(metadatum.first),
                                   ToString(metadatum.second));
    }
    // Terminate rpc with error and debug info in trailer.
    if (request->param().debug_info().stack_entries_size() ||
        !request->param().debug_info().detail().empty()) {
      grpc::string serialized_debug_info =
          request->param().debug_info().SerializeAsString();
      context->AddTrailingMetadata(kDebugInfoTrailerKey, serialized_debug_info);
      return Status::CANCELLED;
    }
  }
  if (request->has_param() &&
      (request->param().expected_client_identity().length() > 0 ||
       request->param().check_auth_context())) {
    CheckServerAuthContext(context,
                           request->param().expected_transport_security_type(),
                           request->param().expected_client_identity());
  }
  if (request->has_param() && request->param().response_message_length() > 0) {
    response->set_message(
        grpc::string(request->param().response_message_length(), '\0'));
  }
  if (request->has_param() && request->param().echo_peer()) {
    response->mutable_param()->set_peer(context->peer());
  }
  return Status::OK;
}

Status TestServiceImpl::CheckClientInitialMetadata(
    ServerContext* context, const SimpleRequest* /*request*/,
    SimpleResponse* /*response*/) {
  EXPECT_EQ(MetadataMatchCount(context->client_metadata(),
                               kCheckClientInitialMetadataKey,
                               kCheckClientInitialMetadataVal),
            1);
  EXPECT_EQ(1u,
            context->client_metadata().count(kCheckClientInitialMetadataKey));
  return Status::OK;
}

// Unimplemented is left unimplemented to test the returned error.

Status TestServiceImpl::RequestStream(ServerContext* context,
                                      ServerReader<EchoRequest>* reader,
                                      EchoResponse* response) {
  // If 'server_try_cancel' is set in the metadata, the RPC is cancelled by
  // the server by calling ServerContext::TryCancel() depending on the value:
  //   CANCEL_BEFORE_PROCESSING: The RPC is cancelled before the server reads
  //   any message from the client
  //   CANCEL_DURING_PROCESSING: The RPC is cancelled while the server is
  //   reading messages from the client
  //   CANCEL_AFTER_PROCESSING: The RPC is cancelled after the server reads
  //   all the messages from the client
  int server_try_cancel = GetIntValueFromMetadata(
      kServerTryCancelRequest, context->client_metadata(), DO_NOT_CANCEL);

  EchoRequest request;
  response->set_message("");

  if (server_try_cancel == CANCEL_BEFORE_PROCESSING) {
    ServerTryCancel(context);
    return Status::CANCELLED;
  }

  std::thread* server_try_cancel_thd = nullptr;
  if (server_try_cancel == CANCEL_DURING_PROCESSING) {
    server_try_cancel_thd =
        new std::thread([context] { ServerTryCancel(context); });
  }

  int num_msgs_read = 0;
  while (reader->Read(&request)) {
    response->mutable_message()->append(request.message());
  }
  gpr_log(GPR_INFO, "Read: %d messages", num_msgs_read);

  if (server_try_cancel_thd != nullptr) {
    server_try_cancel_thd->join();
    delete server_try_cancel_thd;
    return Status::CANCELLED;
  }

  if (server_try_cancel == CANCEL_AFTER_PROCESSING) {
    ServerTryCancel(context);
    return Status::CANCELLED;
  }

  return Status::OK;
}

// Return 'kNumResponseStreamMsgs' messages.
// TODO(yangg) make it generic by adding a parameter into EchoRequest
Status TestServiceImpl::ResponseStream(ServerContext* context,
                                       const EchoRequest* request,
                                       ServerWriter<EchoResponse>* writer) {
  // If server_try_cancel is set in the metadata, the RPC is cancelled by the
  // server by calling ServerContext::TryCancel() depending on the value:
  //   CANCEL_BEFORE_PROCESSING: The RPC is cancelled before the server writes
  //   any messages to the client
  //   CANCEL_DURING_PROCESSING: The RPC is cancelled while the server is
  //   writing messages to the client
  //   CANCEL_AFTER_PROCESSING: The RPC is cancelled after the server writes
  //   all the messages to the client
  int server_try_cancel = GetIntValueFromMetadata(
      kServerTryCancelRequest, context->client_metadata(), DO_NOT_CANCEL);

  int server_coalescing_api = GetIntValueFromMetadata(
      kServerUseCoalescingApi, context->client_metadata(), 0);

  int server_responses_to_send = GetIntValueFromMetadata(
      kServerResponseStreamsToSend, context->client_metadata(),
      kServerDefaultResponseStreamsToSend);

  if (server_try_cancel == CANCEL_BEFORE_PROCESSING) {
    ServerTryCancel(context);
    return Status::CANCELLED;
  }

  EchoResponse response;
  std::thread* server_try_cancel_thd = nullptr;
  if (server_try_cancel == CANCEL_DURING_PROCESSING) {
    server_try_cancel_thd =
        new std::thread([context] { ServerTryCancel(context); });
  }

  for (int i = 0; i < server_responses_to_send; i++) {
    response.set_message(request->message() + grpc::to_string(i));
    if (i == server_responses_to_send - 1 && server_coalescing_api != 0) {
      writer->WriteLast(response, WriteOptions());
    } else {
      writer->Write(response);
    }
  }

  if (server_try_cancel_thd != nullptr) {
    server_try_cancel_thd->join();
    delete server_try_cancel_thd;
    return Status::CANCELLED;
  }

  if (server_try_cancel == CANCEL_AFTER_PROCESSING) {
    ServerTryCancel(context);
    return Status::CANCELLED;
  }

  return Status::OK;
}

Status TestServiceImpl::BidiStream(
    ServerContext* context,
    ServerReaderWriter<EchoResponse, EchoRequest>* stream) {
  // If server_try_cancel is set in the metadata, the RPC is cancelled by the
  // server by calling ServerContext::TryCancel() depending on the value:
  //   CANCEL_BEFORE_PROCESSING: The RPC is cancelled before the server reads/
  //   writes any messages from/to the client
  //   CANCEL_DURING_PROCESSING: The RPC is cancelled while the server is
  //   reading/writing messages from/to the client
  //   CANCEL_AFTER_PROCESSING: The RPC is cancelled after the server
  //   reads/writes all messages from/to the client
  int server_try_cancel = GetIntValueFromMetadata(
      kServerTryCancelRequest, context->client_metadata(), DO_NOT_CANCEL);

  EchoRequest request;
  EchoResponse response;

  if (server_try_cancel == CANCEL_BEFORE_PROCESSING) {
    ServerTryCancel(context);
    return Status::CANCELLED;
  }

  std::thread* server_try_cancel_thd = nullptr;
  if (server_try_cancel == CANCEL_DURING_PROCESSING) {
    server_try_cancel_thd =
        new std::thread([context] { ServerTryCancel(context); });
  }

  // kServerFinishAfterNReads suggests after how many reads, the server should
  // write the last message and send status (coalesced using WriteLast)
  int server_write_last = GetIntValueFromMetadata(
      kServerFinishAfterNReads, context->client_metadata(), 0);

  int read_counts = 0;
  while (stream->Read(&request)) {
    read_counts++;
    gpr_log(GPR_INFO, "recv msg %s", request.message().c_str());
    response.set_message(request.message());
    if (read_counts == server_write_last) {
      stream->WriteLast(response, WriteOptions());
    } else {
      stream->Write(response);
    }
  }

  if (server_try_cancel_thd != nullptr) {
    server_try_cancel_thd->join();
    delete server_try_cancel_thd;
    return Status::CANCELLED;
  }

  if (server_try_cancel == CANCEL_AFTER_PROCESSING) {
    ServerTryCancel(context);
    return Status::CANCELLED;
  }

  return Status::OK;
}

experimental::ServerUnaryReactor* CallbackTestServiceImpl::Echo(
    experimental::CallbackServerContext* context, const EchoRequest* request,
    EchoResponse* response) {
  class Reactor : public ::grpc::experimental::ServerUnaryReactor {
   public:
    Reactor(CallbackTestServiceImpl* service,
            experimental::CallbackServerContext* ctx,
            const EchoRequest* request, EchoResponse* response)
        : service_(service), ctx_(ctx), req_(request), resp_(response) {
      // It should be safe to call IsCancelled here, even though we don't know
      // the result. Call it asynchronously to see if we trigger any data races.
      // Join it in OnDone (technically that could be blocking but shouldn't be
      // for very long).
      async_cancel_check_ = std::thread([this] { (void)ctx_->IsCancelled(); });

      started_ = true;

      if (request->has_param() &&
          request->param().server_notify_client_when_started()) {
        service->signaller_.SignalClientThatRpcStarted();
        // Block on the "wait to continue" decision in a different thread since
        // we can't tie up an EM thread with blocking events. We can join it in
        // OnDone since it would definitely be done by then.
        rpc_wait_thread_ = std::thread([this] {
          service_->signaller_.ServerWaitToContinue();
          StartRpc();
        });
      } else {
        StartRpc();
      }
    }

    void StartRpc() {
      if (req_->has_param() && req_->param().server_sleep_us() > 0) {
        // Set an alarm for that much time
        alarm_.experimental().Set(
            gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                         gpr_time_from_micros(req_->param().server_sleep_us(),
                                              GPR_TIMESPAN)),
            [this](bool ok) { NonDelayed(ok); });
      } else {
        NonDelayed(true);
      }
    }
    void OnSendInitialMetadataDone(bool ok) override {
      EXPECT_TRUE(ok);
      initial_metadata_sent_ = true;
    }
    void OnCancel() override {
      EXPECT_TRUE(started_);
      EXPECT_TRUE(ctx_->IsCancelled());
      // do the actual finish in the main handler only but use this as a chance
      // to cancel any alarms.
      alarm_.Cancel();
      on_cancel_invoked_ = true;
    }
    void OnDone() override {
      if (req_->has_param() && req_->param().echo_metadata_initially()) {
        EXPECT_TRUE(initial_metadata_sent_);
      }
      EXPECT_EQ(ctx_->IsCancelled(), on_cancel_invoked_);
      async_cancel_check_.join();
      if (rpc_wait_thread_.joinable()) {
        rpc_wait_thread_.join();
      }
      delete this;
    }

   private:
    void NonDelayed(bool ok) {
      if (!ok) {
        EXPECT_TRUE(ctx_->IsCancelled());
        Finish(Status::CANCELLED);
        return;
      }
      if (req_->has_param() && req_->param().server_die()) {
        gpr_log(GPR_ERROR, "The request should not reach application handler.");
        GPR_ASSERT(0);
      }
      if (req_->has_param() && req_->param().has_expected_error()) {
        const auto& error = req_->param().expected_error();
        Finish(Status(static_cast<StatusCode>(error.code()),
                      error.error_message(), error.binary_error_details()));
        return;
      }
      int server_try_cancel = GetIntValueFromMetadata(
          kServerTryCancelRequest, ctx_->client_metadata(), DO_NOT_CANCEL);
      if (server_try_cancel != DO_NOT_CANCEL) {
        // Since this is a unary RPC, by the time this server handler is called,
        // the 'request' message is already read from the client. So the
        // scenarios in server_try_cancel don't make much sense. Just cancel the
        // RPC as long as server_try_cancel is not DO_NOT_CANCEL
        EXPECT_FALSE(ctx_->IsCancelled());
        ctx_->TryCancel();
        gpr_log(GPR_INFO, "Server called TryCancel() to cancel the request");
        LoopUntilCancelled(1000);
        return;
      }
      gpr_log(GPR_DEBUG, "Request message was %s", req_->message().c_str());
      resp_->set_message(req_->message());
      MaybeEchoDeadline(ctx_, req_, resp_);
      if (service_->host_) {
        resp_->mutable_param()->set_host(*service_->host_);
      }
      if (req_->has_param() && req_->param().client_cancel_after_us()) {
        {
          std::unique_lock<std::mutex> lock(service_->mu_);
          service_->signal_client_ = true;
        }
        LoopUntilCancelled(req_->param().client_cancel_after_us());
        return;
      } else if (req_->has_param() && req_->param().server_cancel_after_us()) {
        alarm_.experimental().Set(
            gpr_time_add(
                gpr_now(GPR_CLOCK_REALTIME),
                gpr_time_from_micros(req_->param().server_cancel_after_us(),
                                     GPR_TIMESPAN)),
            [this](bool) { Finish(Status::CANCELLED); });
        return;
      } else if (!req_->has_param() || !req_->param().skip_cancelled_check()) {
        EXPECT_FALSE(ctx_->IsCancelled());
      }

      if (req_->has_param() && req_->param().echo_metadata_initially()) {
        const std::multimap<grpc::string_ref, grpc::string_ref>&
            client_metadata = ctx_->client_metadata();
        for (const auto& metadatum : client_metadata) {
          ctx_->AddInitialMetadata(ToString(metadatum.first),
                                   ToString(metadatum.second));
        }
        StartSendInitialMetadata();
      }

      if (req_->has_param() && req_->param().echo_metadata()) {
        const std::multimap<grpc::string_ref, grpc::string_ref>&
            client_metadata = ctx_->client_metadata();
        for (const auto& metadatum : client_metadata) {
          ctx_->AddTrailingMetadata(ToString(metadatum.first),
                                    ToString(metadatum.second));
        }
        // Terminate rpc with error and debug info in trailer.
        if (req_->param().debug_info().stack_entries_size() ||
            !req_->param().debug_info().detail().empty()) {
          grpc::string serialized_debug_info =
              req_->param().debug_info().SerializeAsString();
          ctx_->AddTrailingMetadata(kDebugInfoTrailerKey,
                                    serialized_debug_info);
          Finish(Status::CANCELLED);
          return;
        }
      }
      if (req_->has_param() &&
          (req_->param().expected_client_identity().length() > 0 ||
           req_->param().check_auth_context())) {
        CheckServerAuthContext(ctx_,
                               req_->param().expected_transport_security_type(),
                               req_->param().expected_client_identity());
      }
      if (req_->has_param() && req_->param().response_message_length() > 0) {
        resp_->set_message(
            grpc::string(req_->param().response_message_length(), '\0'));
      }
      if (req_->has_param() && req_->param().echo_peer()) {
        resp_->mutable_param()->set_peer(ctx_->peer());
      }
      Finish(Status::OK);
    }
    void LoopUntilCancelled(int loop_delay_us) {
      if (!ctx_->IsCancelled()) {
        alarm_.experimental().Set(
            gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                         gpr_time_from_micros(loop_delay_us, GPR_TIMESPAN)),
            [this, loop_delay_us](bool ok) {
              if (!ok) {
                EXPECT_TRUE(ctx_->IsCancelled());
              }
              LoopUntilCancelled(loop_delay_us);
            });
      } else {
        Finish(Status::CANCELLED);
      }
    }

    CallbackTestServiceImpl* const service_;
    experimental::CallbackServerContext* const ctx_;
    const EchoRequest* const req_;
    EchoResponse* const resp_;
    Alarm alarm_;
    bool initial_metadata_sent_{false};
    bool started_{false};
    bool on_cancel_invoked_{false};
    std::thread async_cancel_check_;
    std::thread rpc_wait_thread_;
  };

  return new Reactor(this, context, request, response);
}

experimental::ServerUnaryReactor*
CallbackTestServiceImpl::CheckClientInitialMetadata(
    experimental::CallbackServerContext* context, const SimpleRequest*,
    SimpleResponse*) {
  class Reactor : public ::grpc::experimental::ServerUnaryReactor {
   public:
    explicit Reactor(experimental::CallbackServerContext* ctx) {
      EXPECT_EQ(MetadataMatchCount(ctx->client_metadata(),
                                   kCheckClientInitialMetadataKey,
                                   kCheckClientInitialMetadataVal),
                1);
      EXPECT_EQ(ctx->client_metadata().count(kCheckClientInitialMetadataKey),
                1u);
      Finish(Status::OK);
    }
    void OnDone() override { delete this; }
  };

  return new Reactor(context);
}

experimental::ServerReadReactor<EchoRequest>*
CallbackTestServiceImpl::RequestStream(
    experimental::CallbackServerContext* context, EchoResponse* response) {
  // If 'server_try_cancel' is set in the metadata, the RPC is cancelled by
  // the server by calling ServerContext::TryCancel() depending on the
  // value:
  //   CANCEL_BEFORE_PROCESSING: The RPC is cancelled before the server
  //   reads any message from the client CANCEL_DURING_PROCESSING: The RPC
  //   is cancelled while the server is reading messages from the client
  //   CANCEL_AFTER_PROCESSING: The RPC is cancelled after the server reads
  //   all the messages from the client
  int server_try_cancel = GetIntValueFromMetadata(
      kServerTryCancelRequest, context->client_metadata(), DO_NOT_CANCEL);
  if (server_try_cancel == CANCEL_BEFORE_PROCESSING) {
    ServerTryCancelNonblocking(context);
    // Don't need to provide a reactor since the RPC is canceled
    return nullptr;
  }

  class Reactor : public ::grpc::experimental::ServerReadReactor<EchoRequest> {
   public:
    Reactor(experimental::CallbackServerContext* ctx, EchoResponse* response,
            int server_try_cancel)
        : ctx_(ctx),
          response_(response),
          server_try_cancel_(server_try_cancel) {
      EXPECT_NE(server_try_cancel, CANCEL_BEFORE_PROCESSING);
      response->set_message("");

      if (server_try_cancel_ == CANCEL_DURING_PROCESSING) {
        ctx->TryCancel();
        // Don't wait for it here
      }
      StartRead(&request_);
      setup_done_ = true;
    }
    void OnDone() override { delete this; }
    void OnCancel() override {
      EXPECT_TRUE(setup_done_);
      EXPECT_TRUE(ctx_->IsCancelled());
      FinishOnce(Status::CANCELLED);
    }
    void OnReadDone(bool ok) override {
      if (ok) {
        response_->mutable_message()->append(request_.message());
        num_msgs_read_++;
        StartRead(&request_);
      } else {
        gpr_log(GPR_INFO, "Read: %d messages", num_msgs_read_);

        if (server_try_cancel_ == CANCEL_DURING_PROCESSING) {
          // Let OnCancel recover this
          return;
        }
        if (server_try_cancel_ == CANCEL_AFTER_PROCESSING) {
          ServerTryCancelNonblocking(ctx_);
          return;
        }
        FinishOnce(Status::OK);
      }
    }

   private:
    void FinishOnce(const Status& s) {
      std::lock_guard<std::mutex> l(finish_mu_);
      if (!finished_) {
        Finish(s);
        finished_ = true;
      }
    }

    experimental::CallbackServerContext* const ctx_;
    EchoResponse* const response_;
    EchoRequest request_;
    int num_msgs_read_{0};
    int server_try_cancel_;
    std::mutex finish_mu_;
    bool finished_{false};
    bool setup_done_{false};
  };

  return new Reactor(context, response, server_try_cancel);
}

// Return 'kNumResponseStreamMsgs' messages.
// TODO(yangg) make it generic by adding a parameter into EchoRequest
experimental::ServerWriteReactor<EchoResponse>*
CallbackTestServiceImpl::ResponseStream(
    experimental::CallbackServerContext* context, const EchoRequest* request) {
  // If 'server_try_cancel' is set in the metadata, the RPC is cancelled by
  // the server by calling ServerContext::TryCancel() depending on the
  // value:
  //   CANCEL_BEFORE_PROCESSING: The RPC is cancelled before the server
  //   reads any message from the client CANCEL_DURING_PROCESSING: The RPC
  //   is cancelled while the server is reading messages from the client
  //   CANCEL_AFTER_PROCESSING: The RPC is cancelled after the server reads
  //   all the messages from the client
  int server_try_cancel = GetIntValueFromMetadata(
      kServerTryCancelRequest, context->client_metadata(), DO_NOT_CANCEL);
  if (server_try_cancel == CANCEL_BEFORE_PROCESSING) {
    ServerTryCancelNonblocking(context);
  }

  class Reactor
      : public ::grpc::experimental::ServerWriteReactor<EchoResponse> {
   public:
    Reactor(experimental::CallbackServerContext* ctx,
            const EchoRequest* request, int server_try_cancel)
        : ctx_(ctx), request_(request), server_try_cancel_(server_try_cancel) {
      server_coalescing_api_ = GetIntValueFromMetadata(
          kServerUseCoalescingApi, ctx->client_metadata(), 0);
      server_responses_to_send_ = GetIntValueFromMetadata(
          kServerResponseStreamsToSend, ctx->client_metadata(),
          kServerDefaultResponseStreamsToSend);
      if (server_try_cancel_ == CANCEL_DURING_PROCESSING) {
        ctx->TryCancel();
      }
      if (server_try_cancel_ != CANCEL_BEFORE_PROCESSING) {
        if (num_msgs_sent_ < server_responses_to_send_) {
          NextWrite();
        }
      }
      setup_done_ = true;
    }
    void OnDone() override { delete this; }
    void OnCancel() override {
      EXPECT_TRUE(setup_done_);
      EXPECT_TRUE(ctx_->IsCancelled());
      FinishOnce(Status::CANCELLED);
    }
    void OnWriteDone(bool /*ok*/) override {
      if (num_msgs_sent_ < server_responses_to_send_) {
        NextWrite();
      } else if (server_coalescing_api_ != 0) {
        // We would have already done Finish just after the WriteLast
      } else if (server_try_cancel_ == CANCEL_DURING_PROCESSING) {
        // Let OnCancel recover this
      } else if (server_try_cancel_ == CANCEL_AFTER_PROCESSING) {
        ServerTryCancelNonblocking(ctx_);
      } else {
        FinishOnce(Status::OK);
      }
    }

   private:
    void FinishOnce(const Status& s) {
      std::lock_guard<std::mutex> l(finish_mu_);
      if (!finished_) {
        Finish(s);
        finished_ = true;
      }
    }

    void NextWrite() {
      response_.set_message(request_->message() +
                            grpc::to_string(num_msgs_sent_));
      if (num_msgs_sent_ == server_responses_to_send_ - 1 &&
          server_coalescing_api_ != 0) {
        num_msgs_sent_++;
        StartWriteLast(&response_, WriteOptions());
        // If we use WriteLast, we shouldn't wait before attempting Finish
        FinishOnce(Status::OK);
      } else {
        num_msgs_sent_++;
        StartWrite(&response_);
      }
    }
    experimental::CallbackServerContext* const ctx_;
    const EchoRequest* const request_;
    EchoResponse response_;
    int num_msgs_sent_{0};
    int server_try_cancel_;
    int server_coalescing_api_;
    int server_responses_to_send_;
    std::mutex finish_mu_;
    bool finished_{false};
    bool setup_done_{false};
  };
  return new Reactor(context, request, server_try_cancel);
}

experimental::ServerBidiReactor<EchoRequest, EchoResponse>*
CallbackTestServiceImpl::BidiStream(
    experimental::CallbackServerContext* context) {
  class Reactor : public ::grpc::experimental::ServerBidiReactor<EchoRequest,
                                                                 EchoResponse> {
   public:
    explicit Reactor(experimental::CallbackServerContext* ctx) : ctx_(ctx) {
      // If 'server_try_cancel' is set in the metadata, the RPC is cancelled by
      // the server by calling ServerContext::TryCancel() depending on the
      // value:
      //   CANCEL_BEFORE_PROCESSING: The RPC is cancelled before the server
      //   reads any message from the client CANCEL_DURING_PROCESSING: The RPC
      //   is cancelled while the server is reading messages from the client
      //   CANCEL_AFTER_PROCESSING: The RPC is cancelled after the server reads
      //   all the messages from the client
      server_try_cancel_ = GetIntValueFromMetadata(
          kServerTryCancelRequest, ctx->client_metadata(), DO_NOT_CANCEL);
      server_write_last_ = GetIntValueFromMetadata(kServerFinishAfterNReads,
                                                   ctx->client_metadata(), 0);
      if (server_try_cancel_ == CANCEL_BEFORE_PROCESSING) {
        ServerTryCancelNonblocking(ctx);
      } else {
        if (server_try_cancel_ == CANCEL_DURING_PROCESSING) {
          ctx->TryCancel();
        }
        StartRead(&request_);
      }
      setup_done_ = true;
    }
    void OnDone() override { delete this; }
    void OnCancel() override {
      EXPECT_TRUE(setup_done_);
      EXPECT_TRUE(ctx_->IsCancelled());
      FinishOnce(Status::CANCELLED);
    }
    void OnReadDone(bool ok) override {
      if (ok) {
        num_msgs_read_++;
        gpr_log(GPR_INFO, "recv msg %s", request_.message().c_str());
        response_.set_message(request_.message());
        if (num_msgs_read_ == server_write_last_) {
          StartWriteLast(&response_, WriteOptions());
          // If we use WriteLast, we shouldn't wait before attempting Finish
        } else {
          StartWrite(&response_);
          return;
        }
      }

      if (server_try_cancel_ == CANCEL_DURING_PROCESSING) {
        // Let OnCancel handle this
      } else if (server_try_cancel_ == CANCEL_AFTER_PROCESSING) {
        ServerTryCancelNonblocking(ctx_);
      } else {
        FinishOnce(Status::OK);
      }
    }
    void OnWriteDone(bool /*ok*/) override {
      std::lock_guard<std::mutex> l(finish_mu_);
      if (!finished_) {
        StartRead(&request_);
      }
    }

   private:
    void FinishOnce(const Status& s) {
      std::lock_guard<std::mutex> l(finish_mu_);
      if (!finished_) {
        Finish(s);
        finished_ = true;
      }
    }

    experimental::CallbackServerContext* const ctx_;
    EchoRequest request_;
    EchoResponse response_;
    int num_msgs_read_{0};
    int server_try_cancel_;
    int server_write_last_;
    std::mutex finish_mu_;
    bool finished_{false};
    bool setup_done_{false};
  };

  return new Reactor(context);
}

}  // namespace testing
}  // namespace grpc