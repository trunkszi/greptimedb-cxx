/** Offline unary health, DDL, columnar/row CRUD and multi-table ingestion.
 * ./build.sh test -R example_01
 * GREPTIMEDB_ENDPOINT=grpc://localhost:4001 adds a read-only live health probe;
 * all DDL and writes always go to the caller-driven in-memory HTTP/2 peer. */
#include "client_example_support.h"
#include <greptimedb/batch/columnar_batch.h>
#include <greptimedb/batch/row_batch.h>

#include <array>
#include <vector>

namespace {
using namespace client_examples;

struct Demo {
  OfflinePeer peer;
  std::vector<std::string_view> observed;
  static void Handle(void *context, MockGrpcServer &server, const MockRequest &event) noexcept {
    if (event.event != MockRequest::Event::kMessage) return;
    auto &self = *static_cast<Demo *>(context);
    if (event.path == kHealth) {
      self.observed.push_back("health");
      if (self.peer.Send(server, event.stream_id, proto::HealthCheckResponse{}))
        self.peer.Finish(server, event.stream_id);
      return;
    }
    proto::GreptimeRequest request;
    if (!self.peer.Require(event.path == kHandle, "expected unary Handle") ||
        !self.peer.Parse(event, request)) return;
    if (!self.peer.Require(request.header().dbname() == "greptime-public",
                           "request scope missing")) return;
    std::uint32_t affected = 0;
    if (request.has_ddl()) {
      const auto &ddl = request.ddl();
      if (ddl.has_create_database()) self.observed.push_back("create_database");
      else if (ddl.has_create_table()) self.observed.push_back("create_table");
      else if (ddl.has_alter_table()) self.observed.push_back("alter_table");
      else if (ddl.has_truncate_table()) self.observed.push_back("truncate_table");
      else if (ddl.has_drop_table()) self.observed.push_back("drop_table");
      else { self.peer.Require(false, "unexpected DDL"); return; }
    } else if (request.has_inserts()) {
      self.observed.push_back(request.inserts().inserts_size() == 2 ? "insert_multi" : "insert");
      for (const auto &insert : request.inserts().inserts()) {
        if (!self.peer.Require(insert.columns_size() == 3 && insert.row_count() == 1,
                               "columnar payload differs")) return;
        affected += insert.row_count();
      }
    } else if (request.has_row_inserts()) {
      self.observed.push_back("insert_rows");
      for (const auto &insert : request.row_inserts().inserts())
        affected += static_cast<std::uint32_t>(insert.rows().rows_size());
    } else if (request.has_deletes()) {
      self.observed.push_back("delete");
      for (const auto &deletion : request.deletes().deletes()) {
        if (!self.peer.Require(deletion.key_columns_size() == 2, "delete must contain tag/time keys"))
          return;
        affected += deletion.row_count();
      }
    } else if (request.has_row_deletes()) {
      self.observed.push_back("delete_rows");
      for (const auto &deletion : request.row_deletes().deletes())
        affected += static_cast<std::uint32_t>(deletion.rows().rows_size());
    } else { self.peer.Require(false, "unexpected request kind"); return; }
    proto::GreptimeResponse response;
    response.mutable_header()->mutable_status()->set_status_code(0);
    response.mutable_affected_rows()->set_value(affected);
    if (self.peer.Send(server, event.stream_id, response)) self.peer.Finish(server, event.stream_id);
  }
};

template <class Batch> bool Fill(Batch &batch) {
  if (!Checked(batch.AddTagColumn("symbol", ColumnDataType::kString), "tag") ||
      !Checked(batch.AddTimestampColumn("time", TimestampUnit::kMillisecond), "time index") ||
      !Checked(batch.AddFieldColumn("price", ColumnDataType::kFloat64), "price")) return false;
  const std::array values{ValueView{std::string_view{"BTC-USDT"}},
      ValueView{TimestampMillisecond{1700000000000}}, ValueView{42.5}};
  return Checked(batch.PushRow(values), "append row");
}
bool Rows(const Result<std::uint32_t> &result, std::uint32_t expected) {
  return Checked(result, "unary mutation") && Check(*result == expected, "affected rows mismatch");
}

bool Run() {
  Demo demo;
  if (!demo.peer.Open(&Demo::Handle, &demo)) return false;
  auto &client = *demo.peer.client;
  auto health = client.ProbeHealth();
  if (!Checked(health, "health check") || !Check(health->healthy, "health failed") ||
      !Rows(client.CreateDatabase("research", true), 0)) return false;
  proto::CreateTableExpr create;
  create.set_table_name("ticks");
  create.set_engine("mito");
  create.set_time_index("time");
  create.set_create_if_not_exists(true);
  for (const auto &[name, type] : std::array{
      std::pair{"symbol", proto::STRING}, std::pair{"time", proto::TIMESTAMP_MILLISECOND},
      std::pair{"price", proto::FLOAT64}}) {
    auto &column = *create.add_column_defs();
    column.set_name(name); column.set_data_type(type); column.set_is_nullable(false);
  }
  create.add_primary_keys("symbol");
  if (!Rows(client.CreateTable(std::move(create)), 0)) return false;
  proto::AlterTableExpr alter;
  alter.set_table_name("ticks");
  auto &ttl = *alter.mutable_set_table_options()->add_table_options();
  ttl.set_key("ttl"); ttl.set_value("7d");
  if (!Rows(client.AlterTable(std::move(alter)), 0)) return false;
  ColumnarBatch ticks("ticks"), trades("trades");
  RowBatch rows("ticks");
  if (!Fill(ticks) || !Fill(trades) || !Fill(rows)) return false;
  MultiTableBatch multi;
  if (!Checked(multi.AddBatch(ticks), "first table") ||
      !Checked(multi.AddBatch(trades), "second table")) return false;
  WriteHints hints;
  if (!Checked(hints.SetAutoCreateTable(true), "auto-create hint") ||
      !Rows(client.InsertBatch(ticks, &hints), 1) || !Rows(client.InsertMultiBatch(multi), 2) ||
      !Rows(client.InsertRows(rows), 1) || !Rows(client.DeleteBatch(ticks), 1) ||
      !Rows(client.DeleteRows(rows), 1) || !Rows(client.TruncateTable("ticks"), 0) ||
      !Rows(client.DropTable("ticks", true), 0)) return false;
  const std::vector<std::string_view> expected{"health", "create_database", "create_table",
      "alter_table", "insert", "insert_multi", "insert_rows", "delete", "delete_rows",
      "truncate_table", "drop_table"};
  if (!Check(demo.observed == expected && !demo.peer.failure, "unary operation sequence differs"))
    return false;
  fmt::print("Offline unary CRUD: {} checked RPCs, multi-table affected rows=2\n", expected.size());
  return LiveProbe();
}
} // namespace
int main() { return Run() ? EXIT_SUCCESS : EXIT_FAILURE; }
