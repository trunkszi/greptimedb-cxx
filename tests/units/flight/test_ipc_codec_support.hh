#pragma once

#include <greptimedb/flight/ipc_codec.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {
namespace flight = greptimedb::flight;
namespace ipc = greptimedb::flight::ipc;
using Column = greptime::v1::Column;
using flight::FlightCompression;
using flight::FlightIpcEncoder;

constexpr std::array kTypes{greptime::v1::BOOLEAN,
                            greptime::v1::INT8,
                            greptime::v1::INT16,
                            greptime::v1::INT32,
                            greptime::v1::INT64,
                            greptime::v1::UINT8,
                            greptime::v1::UINT16,
                            greptime::v1::UINT32,
                            greptime::v1::UINT64,
                            greptime::v1::FLOAT32,
                            greptime::v1::FLOAT64,
                            greptime::v1::BINARY,
                            greptime::v1::STRING,
                            greptime::v1::DATE,
                            greptime::v1::DATETIME,
                            greptime::v1::TIMESTAMP_SECOND,
                            greptime::v1::TIMESTAMP_MILLISECOND,
                            greptime::v1::TIMESTAMP_MICROSECOND,
                            greptime::v1::TIMESTAMP_NANOSECOND,
                            greptime::v1::DECIMAL128,
                            greptime::v1::JSON};

[[maybe_unused]] Column MakeColumn(greptime::v1::ColumnDataType type, std::size_t rows, bool nullable = true) {
  Column column;
  column.set_column_name("c" + std::to_string(static_cast<int>(type)));
  column.set_datatype(type);
  column.set_semantic_type(type == greptime::v1::STRING ? greptime::v1::TAG : greptime::v1::FIELD);
  if (type == greptime::v1::DECIMAL128) {
    auto *decimal = column.mutable_datatype_extension()->mutable_decimal_type();
    decimal->set_precision(38);
    decimal->set_scale(7);
  }
  std::string mask((rows + 7) / 8, '\0');
  for (std::size_t row = 0; row < rows; ++row) {
    if (nullable && (row == 1 || row == 7 || row == 8)) {
      mask[row / 8] |= static_cast<char>(1U << (row % 8));
      continue;
    }
    auto *values = column.mutable_values();
    const auto n = static_cast<int64_t>(row);
    if (type == greptime::v1::BOOLEAN) values->add_bool_values(row % 2 == 0);
    if (type == greptime::v1::INT8) values->add_i8_values(-128 + n);
    if (type == greptime::v1::INT16) values->add_i16_values(-32768 + n);
    if (type == greptime::v1::INT32) values->add_i32_values(-1234567 + n);
    if (type == greptime::v1::INT64) values->add_i64_values(std::numeric_limits<int64_t>::min() + n);
    if (type == greptime::v1::UINT8) values->add_u8_values(255 - n);
    if (type == greptime::v1::UINT16) values->add_u16_values(65535 - n);
    if (type == greptime::v1::UINT32) values->add_u32_values(std::numeric_limits<uint32_t>::max() - n);
    if (type == greptime::v1::UINT64) values->add_u64_values(std::numeric_limits<uint64_t>::max() - n);
    if (type == greptime::v1::FLOAT32) values->add_f32_values(-0.125F + n);
    if (type == greptime::v1::FLOAT64) values->add_f64_values(-0.0625 + n);
    if (type == greptime::v1::BINARY) values->add_binary_values(std::string{"\0\xff", 2});
    if (type == greptime::v1::JSON) values->add_binary_values("{\"n\":" + std::to_string(n) + "}");
    if (type == greptime::v1::STRING) values->add_string_values("host-" + std::to_string(n));
    if (type == greptime::v1::DATE) values->add_date_values(-1234 + n);
    if (type == greptime::v1::DATETIME) values->add_datetime_values(-86400000 + n);
    if (type == greptime::v1::TIMESTAMP_SECOND) values->add_timestamp_second_values(-3600 + n);
    if (type == greptime::v1::TIMESTAMP_MILLISECOND) values->add_timestamp_millisecond_values(-3600000 + n);
    if (type == greptime::v1::TIMESTAMP_MICROSECOND) values->add_timestamp_microsecond_values(-3600000000LL + n);
    if (type == greptime::v1::TIMESTAMP_NANOSECOND) values->add_timestamp_nanosecond_values(-3600000000000LL + n);
    if (type == greptime::v1::DECIMAL128) {
      auto *decimal = values->add_decimal128_values();
      decimal->set_hi(-1);
      decimal->set_lo(-123456789 + n);
    }
  }
  if (nullable && rows > 1) column.set_null_mask(mask);
  return column;
}

uint64_t ReadLittle(std::span<const uint8_t> bytes, std::size_t at, std::size_t width) {
  uint64_t value = 0;
  for (std::size_t i = 0; i < width; ++i)
    value |= uint64_t{bytes[at + i]} << (8 * i);
  return value;
}

void WriteLittle(std::vector<uint8_t> &bytes, std::size_t at, uint64_t value, std::size_t width) {
  for (std::size_t i = 0; i < width; ++i)
    bytes[at + i] = value >> (8 * i);
}

// Test-side navigation uses the Arrow FlatBuffers field numbers, independent of
// the decoder.
[[maybe_unused]] std::size_t FieldAt(const std::vector<uint8_t> &bytes, std::size_t table, unsigned field) {
  const auto distance = static_cast<int32_t>(ReadLittle(bytes, table, 4));
  const auto vtable = static_cast<std::size_t>(static_cast<int64_t>(table) - distance);
  return table + ReadLittle(bytes, vtable + 4 + field * 2, 2);
}

[[maybe_unused]] std::size_t Indirect(const std::vector<uint8_t> &bytes, std::size_t at) {
  return at + ReadLittle(bytes, at, 4);
}

[[maybe_unused]] std::vector<uint8_t> Encapsulate(const std::vector<uint8_t> &raw, bool marker) {
  std::vector<uint8_t> result(marker ? 8 : 4);
  if (marker) WriteLittle(result, 0, 0xffffffffU, 4);
  WriteLittle(result, marker ? 4 : 0, raw.size(), 4);
  result.insert(result.end(), raw.begin(), raw.end());
  return result;
}

[[nodiscard, maybe_unused]] bool WriteBinaryFixture(std::string_view directory, std::string_view file,
                                                    std::span<const std::uint8_t> bytes) {
  const std::string path = std::string(directory) + "/" + std::string(file);
  std::FILE *output = std::fopen(path.c_str(), "wb");
  if (!output) return false;
  const bool written = bytes.empty() || std::fwrite(bytes.data(), 1, bytes.size(), output) == bytes.size();
  return std::fclose(output) == 0 && written;
}

[[maybe_unused]] void ExpectColumns(const flight::RecordBatch &batch, const std::vector<Column> &columns,
                                    std::size_t rows) {
  ASSERT_EQ(batch.row_count, rows);
  ASSERT_EQ(batch.columns.size(), columns.size());
  for (std::size_t i = 0; i < columns.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(batch.columns[i].column_name(), columns[i].column_name());
    EXPECT_EQ(batch.columns[i].datatype(), columns[i].datatype());
    EXPECT_EQ(batch.columns[i].semantic_type(), columns[i].semantic_type());
    EXPECT_EQ(batch.columns[i].null_mask(), columns[i].null_mask());
    EXPECT_EQ(batch.columns[i].values().SerializeAsString(), columns[i].values().SerializeAsString());
  }
}
} // namespace
