/** SQL DoGet with real Zstd-compressed Arrow IPC, metadata-only DML results,
 * and PromQL instant/range queries. Both HTTP/2 peers progress on this thread.
 * ./build.sh test -R example_07
 * GREPTIMEDB_ENDPOINT adds a live health probe without executing live writes. */
#include "client_example_support.h"
#include <greptimedb/flight/ipc_codec.h>

#include <array>
#include <vector>

namespace {
using namespace client_examples;
namespace af = arrow::flight::protocol;
namespace flight = greptimedb::flight;

struct Demo {
  OfflinePeer peer;
  flight::EncodedFlightBody schema, data, none;
  std::size_t selects{}, mutations{}, instant{}, ranges{};

  bool Prepare() {
    std::array<proto::Column, 2> columns;
    columns[0].set_column_name("time");
    columns[0].set_datatype(proto::TIMESTAMP_MILLISECOND);
    columns[0].set_semantic_type(proto::TIMESTAMP);
    columns[1].set_column_name("latency");
    columns[1].set_datatype(proto::FLOAT64);
    columns[1].set_semantic_type(proto::FIELD);
    for (int row = 0; row < 3; ++row) {
      columns[0].mutable_values()->add_timestamp_millisecond_values(1700000000000 + row * 1000);
      columns[1].mutable_values()->add_f64_values(1.25 + row);
    }
    auto batch = flight::RecordBatchFromProtoColumns(columns, 3);
    if (!Checked(batch, "columnar to Arrow")) return false;
    flight::FlightIpcEncoder encoder(flight::FlightCompression::kZstd);
    auto encoded_schema = encoder.EncodeSchema(batch->schema);
    auto encoded_data = encoder.EncodeBatch(*batch);
    auto encoded_none = flight::EncodeMetadataMessage();
    if (!Checked(encoded_schema, "IPC schema") || !Checked(encoded_data, "Zstd IPC body") ||
        !Checked(encoded_none, "metadata IPC header")) return false;
    auto header = flight::ipc::DecodeMessage(encoded_data->data_header);
    if (!Checked(header, "inspect IPC header") || !Check(header->batch.compression ==
        flight::ipc::Compression::kZstd, "body must advertise Zstd IPC compression")) return false;
    schema = std::move(*encoded_schema); data = std::move(*encoded_data);
    none = std::move(*encoded_none);
    return true;
  }
  bool SendBody(MockGrpcServer &server, std::int32_t id,
                const flight::EncodedFlightBody &body, std::string_view app = {}) {
    af::FlightData frame;
    frame.set_data_header(body.data_header.data(), body.data_header.size());
    if (!body.data_body.empty()) frame.set_data_body(body.data_body.data(), body.data_body.size());
    if (!app.empty()) frame.set_app_metadata(app.data(), app.size());
    return peer.Send(server, id, frame);
  }
  static void Handle(void *context, MockGrpcServer &server, const MockRequest &event) noexcept {
    if (event.event != MockRequest::Event::kMessage) return;
    auto &self = *static_cast<Demo *>(context);
    if (event.path == kProm) {
      proto::PromqlRequest query;
      if (!self.peer.Parse(event, query)) return;
      const bool range = query.has_range_query();
      if (range) {
        if (!self.peer.Require(query.range_query().query() == "up" &&
            query.range_query().step() == "15s", "range request differs")) return;
        ++self.ranges;
      } else {
        if (!self.peer.Require(query.has_instant_query() && query.instant_query().query() == "up",
                               "instant request differs")) return;
        ++self.instant;
      }
      proto::PromqlResponse response;
      response.mutable_header()->mutable_status()->set_status_code(0);
      response.set_body(range ? R"({"status":"success","data":{"resultType":"matrix","result":[]}})"
                              : R"({"status":"success","data":{"resultType":"vector","result":[]}})");
      if (self.peer.Send(server, event.stream_id, response)) self.peer.Finish(server, event.stream_id);
      return;
    }
    af::Ticket ticket;
    proto::GreptimeRequest request;
    if (!self.peer.Require(event.path == kDoGet, "expected Flight DoGet") ||
        !self.peer.Parse(event, ticket) ||
        !self.peer.Require(request.ParseFromString(ticket.ticket()) && request.has_query(),
                           "DoGet ticket must contain a scoped query")) return;
    if (!self.peer.Require(request.header().dbname() == "greptime-public", "missing query scope"))
      return;
    if (request.query().sql() == "SELECT time, latency FROM metrics") {
      ++self.selects;
      if (!self.SendBody(server, event.stream_id, self.schema) ||
          !self.SendBody(server, event.stream_id, self.data)) return;
    } else if (request.query().sql() == "DELETE FROM metrics WHERE latency > 10") {
      ++self.mutations;
      proto::FlightMetadata metadata;
      metadata.mutable_affected_rows()->set_value(7);
      metadata.mutable_metrics()->set_metrics(R"({"elapsed_ms":2})");
      std::string app;
      if (!self.peer.Require(metadata.SerializeToString(&app), "DML metadata encoding") ||
          !self.SendBody(server, event.stream_id, self.none, app)) return;
    } else { self.peer.Require(false, "unexpected SQL"); return; }
    self.peer.Finish(server, event.stream_id);
  }
};

bool Run() {
  Demo demo;
  if (!demo.Prepare() || !demo.peer.Open(&Demo::Handle, &demo)) return false;
  auto &client = *demo.peer.client;
  {
    auto query = client.Query("SELECT time, latency FROM metrics");
    if (!Checked(query, "SQL DoGet")) return false;
    auto batch = (*query)->NextBatch();
    if (!Checked(batch, "read compressed Arrow batch") || !Check(batch->has_value(), "missing rows"))
      return false;
    const auto &decoded = **batch;
    if (!Check(decoded.row_count == 3 && decoded.columns.size() == 2, "SQL shape differs") ||
        !Check(decoded.columns[1].values().f64_values(0) == 1.25 &&
               decoded.columns[1].values().f64_values(2) == 3.25, "decoded values differ")) return false;
    auto eof = (*query)->NextBatch();
    if (!Checked(eof, "SQL EOF") || !Check(!eof->has_value(), "unexpected extra batch") ||
        !Checked((*query)->Finish(), "SQL trailers")) return false;
    fmt::print("SQL: {} rows decoded from Zstd-compressed IPC bodies\n", decoded.row_count);
  }
  {
    auto query = client.Query("DELETE FROM metrics WHERE latency > 10");
    if (!Checked(query, "DML DoGet")) return false;
    auto result = (*query)->Finish();
    if (!Checked(result, "DML metadata/trailers") ||
        !Check(result->affected_rows == 7 && result->metrics.size() == 1 &&
            result->metrics[0] == R"({"elapsed_ms":2})", "DML summary differs")) return false;
    fmt::print("DML: {} affected rows, metrics={}\n", *result->affected_rows, result->metrics[0]);
  }
  auto instant = client.PromInstantQuery("up", "1700000000", "5m");
  auto range = client.PromRangeQuery("up", "1700000000", "1700000060", "15s", "5m");
  if (!Checked(instant, "PromQL instant") || !Checked(range, "PromQL range")) return false;
  auto instant_json = instant->AsJson(); auto range_json = range->AsJson();
  if (!Checked(instant_json, "instant UTF-8 body") || !Checked(range_json, "range UTF-8 body") ||
      !Check(instant_json->find("vector") != std::string_view::npos &&
             range_json->find("matrix") != std::string_view::npos, "PromQL JSON result types differ") ||
      !Check(demo.selects == 1 && demo.mutations == 1 && demo.instant == 1 && demo.ranges == 1 &&
             !demo.peer.failure, "request counters differ")) return false;
  fmt::print("PromQL: instant vector and range matrix returned through Client\n");
  return LiveProbe();
}
} // namespace
int main() { return Run() ? EXIT_SUCCESS : EXIT_FAILURE; }
