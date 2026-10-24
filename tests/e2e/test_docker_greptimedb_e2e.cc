#include "docker_e2e_support.h"
#include "live_type_matrix.h"

#include <thread>

namespace {
using namespace greptimedb;
using namespace greptimedb::e2e;
namespace proto = greptime::v1;
struct[[= table{.name = "typed_ticks"}]] Tick {
  [[= tag{}]] std::string host;
  [[= timestamp{.unit = TimestampUnit::kMillisecond}]] std::int64_t ts;
  std::optional<double> value;
};
struct Projection {
  [[= timestamp{.unit = TimestampUnit::kMillisecond}]] std::int64_t ts;
  std::optional<double> value;
};
static_assert(BatchEncodeConcept<TypedColumnarBatch<Tick>>);

auto Table(std::string_view schema, std::string_view name)
    -> proto::CreateTableExpr {
  proto::CreateTableExpr table;
  table.set_catalog_name("greptime");
  table.set_schema_name(std::string(schema));
  table.set_table_name(std::string(name));
  table.set_time_index("ts");
  table.add_primary_keys("host");
  table.set_engine("mito");
  auto add = [&](const char *column, proto::ColumnDataType type,
                 proto::SemanticType role, bool nullable) {
    auto *definition = table.add_column_defs();
    definition->set_name(column);
    definition->set_data_type(type);
    definition->set_semantic_type(role);
    definition->set_is_nullable(nullable);
  };
  add("host", proto::STRING, proto::TAG, false);
  add("ts", proto::TIMESTAMP_MILLISECOND, proto::TIMESTAMP, false);
  add("value", proto::FLOAT64, proto::FIELD, true);
  return table;
}
template <class Batch>
auto Fill(Batch &batch, std::int64_t offset, double value) -> Result<void> {
  if (auto added = batch.AddTagColumn("host", ColumnDataType::kString); !added)
    return std::unexpected(added.error());
  if (auto added = batch.AddTimestampColumn("ts", TimestampUnit::kMillisecond);
      !added)
    return std::unexpected(added.error());
  if (auto added = batch.AddFieldColumn("value", ColumnDataType::kFloat64);
      !added)
    return std::unexpected(added.error());
  return batch.PushRow(std::array<ValueView, 3>{
      std::string_view{"local"}, TimestampMillisecond{kTs + offset}, value});
}
using Rows = std::vector<std::pair<std::int64_t, double>>;
auto ReadRows(Client &client, std::string_view table,
              std::string_view filter = {}) -> Result<Rows> {
  auto query = client.Query("SELECT ts, value FROM " + std::string(table) +
                            std::string(filter) + " ORDER BY ts");
  if (!query)
    return std::unexpected(query.error());
  Rows rows;
  while (true) {
    auto batch = (*query)->NextBatch();
    if (!batch)
      return std::unexpected(batch.error());
    if (!*batch)
      break;
    if ((**batch).columns.size() != 2)
      return std::unexpected(
          Error{ErrorKind::kDecode, "Unexpected live RecordBatch shape"});
    for (std::size_t row = 0; row < (**batch).row_count; ++row) {
      auto view = ViewDecodedRow<Projection>((**batch).columns, row,
                                             (**batch).row_count);
      if (!view || !view->Get<1>())
        return std::unexpected(
            Error{ErrorKind::kDecode, "Invalid live RowView projection"});
      rows.emplace_back(view->Get<0>(), *view->Get<1>());
    }
  }
  auto finished = (*query)->Finish();
  if (!finished)
    return std::unexpected(finished.error());
  return rows;
}
class DockerGreptimeDbE2E : public ::testing::Test {
protected:
  std::unique_ptr<Client> root, connection;
  std::unique_ptr<DatabaseCleanup> cleanup;
  std::string schema;
  static Session &Server() {
    static Session session;
    static const bool prepared = (session.Prepare(), true);
    static_cast<void>(prepared);
    return session;
  }
  void SetUp() override {
    auto &session = Server();
    if (!session.error.empty()) {
      if (session.unavailable && !Enabled("GREPTIMEDB_E2E_REQUIRED"))
        GTEST_SKIP() << session.error;
      FAIL() << session.error;
    }
    auto ready = WaitReady(session);
    ASSERT_TRUE(ready) << ready.error().Message();
    root = std::move(*ready);
    ASSERT_TRUE(root->Ping());
    auto health = root->ProbeHealth();
    ASSERT_TRUE(health) << health.error().Message();
    ASSERT_TRUE(health->healthy);
    schema = "cxx_e2e_" + std::to_string(::getpid()) + "_" +
             std::to_string(
                 std::chrono::system_clock::now().time_since_epoch().count());
    auto created = root->CreateDatabase(schema, false);
    ASSERT_TRUE(created) << created.error().Message();
    cleanup.reset(new (std::nothrow) DatabaseCleanup{*root, schema});
    ASSERT_NE(cleanup, nullptr);
    auto connected = Connect(session.endpoint, schema);
    ASSERT_TRUE(connected) << connected.error().Message();
    connection = std::move(*connected);
    std::cout << "Live endpoint=" << session.endpoint << " schema=" << schema
              << '\n';
  }
  void TearDown() override {
    connection.reset();
    if (cleanup && cleanup->active) {
      auto cleaned = cleanup->Run();
      EXPECT_TRUE(cleaned) << cleaned.error().Message();
    }
    cleanup.reset();
  }
};

TEST_F(DockerGreptimeDbE2E, AllColumnTypesAndMultiwordNullMasks) {
  auto &client = *connection;
  auto created = client.CreateTable(MatrixTable(schema));
  ASSERT_TRUE(created) << created.error().Message();
  ColumnarBatch matrix("type_matrix", 130);
  ASSERT_TRUE(AddMatrixColumns(matrix));
  for (std::size_t row = 0; row < 130; ++row) {
    std::array<ValueView, 42> values;
    values[0] = std::string_view{"matrix"};
    values[1] = TimestampMillisecond{kTs + static_cast<std::int64_t>(row)};
    const auto scalar = MatrixValues(row);
    for (std::size_t i = 0; i < scalar.size(); ++i) {
      values[2 + 2 * i] = scalar[i];
      values[3 + 2 * i] =
          MatrixNull(row, i) ? ValueView{std::monostate{}} : scalar[i];
    }
    ASSERT_TRUE(matrix.PushRow(values));
  }
  auto encoded = matrix.EncodeInsertRequest();
  ASSERT_TRUE(encoded);
  for (int column = 3; column < encoded->columns_size(); column += 2) {
    const auto &mask = encoded->columns(column).null_mask();
    ASSERT_EQ(mask.size(), 17U);
    EXPECT_NE(static_cast<unsigned char>(mask[7]) & 0x80U, 0U);
    EXPECT_NE(static_cast<unsigned char>(mask[8]) & 0x01U, 0U);
    EXPECT_NE(static_cast<unsigned char>(mask[16]) & 0x02U, 0U);
  }
  auto inserted = client.InsertBatch(matrix);
  ASSERT_TRUE(inserted) << inserted.error().Message();
  ASSERT_EQ(*inserted, 130U);
  auto query = client.Query(MatrixSql());
  ASSERT_TRUE(query) << query.error().Message();
  std::size_t seen = 0;
  for (;;) {
    auto result = (*query)->NextBatch();
    ASSERT_TRUE(result) << result.error().Message();
    if (!*result)
      break;
    const auto &batch = **result;
    ASSERT_EQ(batch.columns.size(), 42U);
    for (std::size_t row = 0; row < batch.row_count; ++row, ++seen) {
      ASSERT_LT(seen, 130U);
      EXPECT_EQ(batch.columns[0].values().string_values(static_cast<int>(row)),
                "matrix");
      EXPECT_EQ(batch.columns[1].values().timestamp_millisecond_values(
                    static_cast<int>(row)),
                kTs + static_cast<std::int64_t>(seen));
      const auto scalar = MatrixValues(seen);
      for (std::size_t i = 0; i < scalar.size(); ++i) {
        SCOPED_TRACE(std::string(kMatrixTypes[i].name) +
                     " row=" + std::to_string(seen));
        for (std::size_t nullable = 0; nullable < 2; ++nullable) {
          auto value = MatrixValue(batch.columns[2 + 2 * i + nullable],
                                   kMatrixTypes[i].type, row, batch.row_count);
          ASSERT_TRUE(value) << value.error().Message();
          const auto expected = nullable && MatrixNull(seen, i)
                                    ? ValueView{std::monostate{}}
                                    : scalar[i];
          EXPECT_TRUE(EqualMatrixValue(*value, expected));
        }
      }
    }
  }
  ASSERT_TRUE((*query)->Finish());
  EXPECT_EQ(seen, 130U);
}

TEST_F(DockerGreptimeDbE2E, LegacyDateTimeServerContract) {
  auto &client = *connection;
  for (bool nullable : {false, true}) {
    auto definition =
        Table(schema, nullable ? "nullable_datetime" : "required_datetime");
    auto &column = *definition.add_column_defs();
    column.set_name("legacy");
    column.set_data_type(proto::DATETIME);
    column.set_semantic_type(proto::FIELD);
    column.set_is_nullable(nullable);
    auto created = client.CreateTable(std::move(definition));
    ASSERT_FALSE(created)
        << "Server now supports DATETIME; add it to the positive type matrix";
    EXPECT_EQ(created.error().Kind(), ErrorKind::kServerStatus);
    EXPECT_NE(
        created.error().Message().find("datetime type, which is not supported"),
        std::string_view::npos)
        << created.error().Message();
    EXPECT_NE(created.error().RawCode(), 0U);
    ASSERT_TRUE(client.Ping());
  }
  ColumnarBatch batch("datetime_insert");
  ASSERT_TRUE(Fill(batch, 0, 0));
  batch.Clear();
  ASSERT_TRUE(batch.AddFieldColumn("legacy", ColumnDataType::kDateTime));
  ASSERT_TRUE(batch.PushRow(std::array<ValueView, 4>{std::string_view{"local"},
                                                     TimestampMillisecond{kTs},
                                                     1.0, DateTime{kTs}}));
  WriteHints hints;
  ASSERT_TRUE(hints.SetAutoCreateTable(true));
  auto inserted = client.InsertBatch(batch, &hints);
  ASSERT_FALSE(inserted);
  EXPECT_EQ(inserted.error().Kind(), ErrorKind::kServerStatus);
  EXPECT_NE(inserted.error().Message().find("datetime"),
            std::string_view::npos);
  ASSERT_TRUE(client.Ping());
}

TEST_F(DockerGreptimeDbE2E, UnaryDdlCrudAndStreaming) {
  auto &client = *connection;
  for (const auto table : {"ticks", "typed_ticks"}) {
    auto result = client.CreateTable(Table(schema, table));
    ASSERT_TRUE(result) << result.error().Message();
  }
  ColumnarBatch dynamic("ticks");
  ASSERT_TRUE(Fill(dynamic, 0, 1.5));
  auto inserted = client.InsertBatch(dynamic);
  ASSERT_TRUE(inserted) << inserted.error().Message();
  EXPECT_EQ(*inserted, 1U);
  TypedColumnarBatch<Tick> typed;
  ASSERT_TRUE(typed.Push(Tick{"local", kTs + 1, 2.5}));
  inserted = client.InsertBatch(typed);
  ASSERT_TRUE(inserted) << inserted.error().Message();
  EXPECT_EQ(*inserted, 1U);
  dynamic.Clear();
  ASSERT_TRUE(dynamic.PushRow(std::array<ValueView, 3>{
      std::string_view{"local"}, TimestampMillisecond{kTs + 2}, 3.5}));
  typed.Clear();
  ASSERT_TRUE(typed.Push(Tick{"local", kTs + 3, 4.5}));
  MultiTableBatch multi;
  ASSERT_TRUE(multi.AddBatch(dynamic));
  ASSERT_TRUE(multi.AddBatch(typed));
  inserted = client.InsertMultiBatch(multi);
  ASSERT_TRUE(inserted) << inserted.error().Message();
  EXPECT_EQ(*inserted, 2U);
  RowBatch rows("ticks");
  ASSERT_TRUE(Fill(rows, 4, 5.5));
  inserted = client.InsertRows(rows);
  ASSERT_TRUE(inserted) << inserted.error().Message();
  EXPECT_EQ(*inserted, 1U);
  {
    auto stream = client.StreamInserter();
    ASSERT_TRUE(stream) << stream.error().Message();
    for (int batch = 0; batch < 4; ++batch) {
      dynamic.Clear();
      for (int row = 0; row < 33; ++row) {
        const int serial = batch * 33 + row;
        ASSERT_TRUE(dynamic.PushRow(std::array<ValueView, 3>{
            std::string_view{"local"}, TimestampMillisecond{kTs + 100 + serial},
            10.0 + serial}));
      }
      auto request = dynamic.EncodeInsertRequest();
      ASSERT_TRUE(request);
      auto sent = (*stream)->SendBatch(*request);
      ASSERT_TRUE(sent) << sent.error().Message();
    }
    EXPECT_EQ((*stream)->RowsSent(), 132U);
    EXPECT_EQ((*stream)->BatchesSent(), 4U);
    auto finished = (*stream)->Finish();
    ASSERT_TRUE(finished) << finished.error().Message();
    EXPECT_EQ(*finished, 132U);
  }
  Rows expected{{kTs, 1.5}, {kTs + 2, 3.5}, {kTs + 4, 5.5}};
  for (int row = 0; row < 132; ++row)
    expected.emplace_back(kTs + 100 + row, 10.0 + row);
  auto actual = ReadRows(client, "ticks");
  ASSERT_TRUE(actual) << actual.error().Message();
  EXPECT_EQ(*actual, expected);
  actual = ReadRows(client, "typed_ticks");
  ASSERT_TRUE(actual) << actual.error().Message();
  EXPECT_EQ(*actual, (Rows{{kTs + 1, 2.5}, {kTs + 3, 4.5}}));
  proto::AlterTableExpr alter;
  alter.set_catalog_name("greptime");
  alter.set_schema_name(schema);
  alter.set_table_name("ticks");
  auto *note =
      alter.mutable_add_columns()->add_add_columns()->mutable_column_def();
  note->set_name("note");
  note->set_data_type(proto::STRING);
  note->set_semantic_type(proto::FIELD);
  note->set_is_nullable(true);
  auto altered = client.AlterTable(alter);
  ASSERT_TRUE(altered) << altered.error().Message();
  auto notes = client.Query("SELECT note FROM ticks");
  ASSERT_TRUE(notes) << notes.error().Message();
  std::size_t nulls{};
  while (true) {
    auto batch = (*notes)->NextBatch();
    ASSERT_TRUE(batch) << batch.error().Message();
    if (!*batch)
      break;
    ASSERT_EQ((**batch).columns.size(), 1U);
    EXPECT_EQ((**batch).columns[0].values().string_values_size(), 0);
    nulls += (**batch).row_count;
  }
  EXPECT_EQ(nulls, expected.size());
  ASSERT_TRUE((*notes)->Finish());
  notes->reset();
  ColumnarBatch delete_batch("ticks");
  ASSERT_TRUE(delete_batch.AddTagColumn("host", ColumnDataType::kString));
  ASSERT_TRUE(
      delete_batch.AddTimestampColumn("ts", TimestampUnit::kMillisecond));
  ASSERT_TRUE(delete_batch.PushRow(std::array<ValueView, 2>{
      std::string_view{"local"}, TimestampMillisecond{kTs}}));
  auto deleted = client.DeleteBatch(delete_batch);
  ASSERT_TRUE(deleted) << deleted.error().Message();
  EXPECT_EQ(*deleted, 1U);
  RowBatch delete_rows("ticks");
  ASSERT_TRUE(delete_rows.AddTagColumn("host", ColumnDataType::kString));
  ASSERT_TRUE(
      delete_rows.AddTimestampColumn("ts", TimestampUnit::kMillisecond));
  ASSERT_TRUE(delete_rows.PushRow(std::array<ValueView, 2>{
      std::string_view{"local"}, TimestampMillisecond{kTs + 4}}));
  deleted = client.DeleteRows(delete_rows);
  ASSERT_TRUE(deleted) << deleted.error().Message();
  EXPECT_EQ(*deleted, 1U);
  actual = ReadRows(client, "ticks");
  ASSERT_TRUE(actual) << actual.error().Message();
  Rows remaining;
  for (const auto &row : expected)
    if (row.first != kTs && row.first != kTs + 4)
      remaining.push_back(row);
  EXPECT_EQ(*actual, remaining);
  for (auto table : {"ticks", "typed_ticks"}) {
    auto truncated = client.TruncateTable(table);
    ASSERT_TRUE(truncated) << truncated.error().Message();
    actual = ReadRows(client, table);
    ASSERT_TRUE(actual) << actual.error().Message();
    EXPECT_TRUE(actual->empty());
    auto dropped = client.DropTable(table, false);
    ASSERT_TRUE(dropped) << dropped.error().Message();
    EXPECT_FALSE(ReadRows(client, table));
  }
}

TEST_F(DockerGreptimeDbE2E, FlightCompressionAckWindowAndTypedReplay) {
  auto &client = *connection;
  for (auto compression :
       {flight::FlightCompression::kNone, flight::FlightCompression::kLz4Frame,
        flight::FlightCompression::kZstd}) {
    const auto name = "bulk_" + std::to_string(static_cast<int>(compression));
    auto created = client.CreateTable(Table(schema, name));
    ASSERT_TRUE(created) << created.error().Message();
    ColumnarBatch source(name);
    ASSERT_TRUE(Fill(source, 0, 0));
    source.Clear();
    auto record = [&]() -> flight::FlightResult<flight::RecordBatch> {
      auto encoded = source.EncodeInsertRequest();
      if (!encoded)
        return std::unexpected(std::string(encoded.error().Message()));
      std::vector<proto::Column> columns;
      for (const auto &column : encoded->columns())
        columns.push_back(column);
      return flight::RecordBatchFromProtoColumns(columns, encoded->row_count());
    };
    Rows expected;
    auto append = [&](int batch) {
      source.Clear();
      for (int row = 0; row < 37; ++row) {
        const int serial = batch * 37 + row;
        ASSERT_TRUE(source.PushRow(std::array<ValueView, 3>{
            std::string_view{"local"}, TimestampMillisecond{kTs + serial},
            0.25 + serial}));
        expected.emplace_back(kTs + serial, 0.25 + serial);
      }
    };
    append(0);
    auto first = record();
    ASSERT_TRUE(first) << first.error();
    flight::BulkWriteOptions options;
    options.compression = compression;
    options.max_in_flight = 2;
    auto writer = client.BulkWriter(name, first->schema, options);
    ASSERT_TRUE(writer) << writer.error().Message();
    for (int batch = 0; batch < 4; ++batch) {
      if (batch != 0)
        append(batch);
      auto data = record();
      ASSERT_TRUE(data) << data.error();
      auto written = (*writer)->WriteRecordBatch(*data);
      ASSERT_TRUE(written) << written.error().Message();
      EXPECT_LE((*writer)->InFlight(), 2U);
    }
    auto stats = (*writer)->Finish();
    ASSERT_TRUE(stats) << stats.error().Message();
    EXPECT_EQ(stats->batches_sent, 4U);
    EXPECT_EQ(stats->total_affected_rows, 148U);
    EXPECT_EQ((*writer)->InFlight(), 0U);
    auto rows = ReadRows(client, name);
    ASSERT_TRUE(rows) << rows.error().Message();
    EXPECT_EQ(*rows, expected);
  }
}

TEST_F(DockerGreptimeDbE2E, StructuredQueryFailurePreservesChannel) {
  auto &client = *connection;
  for (int attempt = 0; attempt < 3; ++attempt) {
    auto missing = ReadRows(client, "missing_table");
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().Kind(), ErrorKind::kServerStatus)
        << missing.error().Message();
    EXPECT_NE(missing.error().RawCode(), 0U);
    EXPECT_NE(missing.error().Message().find("missing_table"),
              std::string_view::npos);
    ASSERT_TRUE(client.Ping());
  }
  ASSERT_TRUE(client.CreateTable(Table(schema, "after_error")));
  ColumnarBatch batch("after_error");
  ASSERT_TRUE(Fill(batch, 1, 7.5));
  auto inserted = client.InsertBatch(batch);
  ASSERT_TRUE(inserted) << inserted.error().Message();
  auto rows = ReadRows(client, "after_error");
  ASSERT_TRUE(rows) << rows.error().Message();
  EXPECT_EQ(*rows, (Rows{{kTs + 1, 7.5}}));
}

auto ConcurrentRoundTrip(Client &client, std::string_view host, int base)
    -> Result<void> {
  ColumnarBatch batch("concurrent_ticks");
  if (auto r = Fill(batch, 0, 0); !r)
    return std::unexpected(r.error());
  batch.Clear();
  Rows expected;
  for (int i = 0; i < 16; ++i) {
    const auto timestamp = kTs + base + i;
    if (auto r = batch.PushRow(std::array<ValueView, 3>{
            host, TimestampMillisecond{timestamp}, 0.5 + base + i});
        !r)
      return std::unexpected(r.error());
    expected.emplace_back(timestamp, 0.5 + base + i);
  }
  auto inserted = client.InsertBatch(batch);
  if (!inserted)
    return std::unexpected(inserted.error());
  if (*inserted != 16)
    return std::unexpected(
        Error{ErrorKind::kProtocol, "concurrent affected rows differ"});
  auto rows = ReadRows(client, "concurrent_ticks",
                       " WHERE host='" + std::string(host) + "'");
  if (!rows)
    return std::unexpected(rows.error());
  if (*rows != expected)
    return std::unexpected(
        Error{ErrorKind::kProtocol, "concurrent query differs"});
  return {};
}

TEST_F(DockerGreptimeDbE2E, ThreadLocalAndWorkerConcurrentIngestion) {
  ASSERT_TRUE(connection->CreateTable(Table(schema, "concurrent_ticks")));
  using Handle = std::unique_ptr<Client>;
  struct Context {
    std::string endpoint, schema;
  } context{Server().endpoint, schema};
  auto factory = [&context](const std::vector<std::string> &endpoints) {
    return Connect(endpoints.front(), context.schema);
  };
  auto local = ThreadLocalClient<Handle, decltype(factory)>::Create(
      {context.endpoint}, factory);
  WorkerConfig config;
  config.slots_per_thread = 2;
  config.balance = WorkerBalance::kRoundRobin;
  auto workers = WorkerClient<Handle>::Create(
      {context.endpoint}, config,
      [&context](const std::vector<std::string> &endpoints, std::uint64_t,
                 std::size_t) {
        return Connect(endpoints.front(), context.schema);
      });
  ASSERT_TRUE(local) << local.error().Message();
  ASSERT_TRUE(workers) << workers.error().Message();
  std::array<std::string, 3> failures;
  std::array<std::thread, 3> threads;
  for (std::size_t thread = 0; thread < threads.size(); ++thread) {
    threads[thread] = std::thread([&, thread] {
      auto run = [&]() -> Result<void> {
        const auto host = "thread_" + std::to_string(thread);
        const Client *identity = nullptr;
        auto write = local->WithClient([&](Handle &client) {
          identity = client.get();
          return ConcurrentRoundTrip(*client, host + "_local",
                                     1000 + static_cast<int>(thread) * 100);
        });
        if (!write)
          return std::unexpected(write.error());
        auto reuse = local->WithClient([&](Handle &client) -> Result<void> {
          if (identity != client.get())
            return std::unexpected(
                Error::InvalidConfig("local client not reused"));
          return client->Ping();
        });
        if (!reuse)
          return std::unexpected(reuse.error());
        std::array<bool, 2> seen{};
        for (int slot = 0; slot < 2; ++slot) {
          auto lease = workers->Lease();
          if (!lease)
            return std::unexpected(lease.error());
          if (lease->slot() >= seen.size())
            return std::unexpected(Error::InvalidConfig("bad worker slot"));
          seen[lease->slot()] = true;
          auto result = ConcurrentRoundTrip(
              *lease->client(), host + "_slot_" + std::to_string(slot),
              10000 + static_cast<int>(thread) * 1000 + slot * 100);
          if (!result)
            return std::unexpected(result.error());
        }
        if (!seen[0] || !seen[1])
          return std::unexpected(
              Error::InvalidConfig("worker slots not visited"));
        return {};
      };
      auto result = run();
      local->CloseCurrentThread();
      workers->CloseCurrentThread();
      if (!result)
        failures[thread] = result.error().Message();
    });
  }
  for (auto &thread : threads)
    thread.join();
  for (const auto &failure : failures)
    EXPECT_TRUE(failure.empty()) << failure;
  auto rows = ReadRows(*connection, "concurrent_ticks");
  ASSERT_TRUE(rows) << rows.error().Message();
  EXPECT_EQ(rows->size(), 144U);
}
} // namespace
