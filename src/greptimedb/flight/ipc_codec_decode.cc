#include "ipc_codec_internal.hh"

#include "ipc_views.hh"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <algorithm>
#include <array>
#include <limits>
#include <uarch/uarch.hpp>

namespace greptimedb::flight {
using namespace codec;
using detail::DecodedBuffer;
using detail::Decompress;

FlightResult<FlightSchema> DecodeFlightSchema(std::span<const uint8_t> header) {
  auto message = ipc::DecodeMessage(header);
  if (!message) return std::unexpected(message.error());
  if (message->header_type != ipc::HeaderType::kSchema || message->body_length != 0)
    return Fail("expected schema frame");
  auto valid = ValidateSchema(message->schema);
  if (!valid) return std::unexpected(valid.error());
  return std::move(message->schema);
}
FlightResult<RecordBatch> DecodeFlightRecordBatch(const FlightSchema &schema, std::span<const uint8_t> header,
                                                  std::span<const uint8_t> body) {
  auto valid = ValidateSchema(schema);
  if (!valid) return std::unexpected(valid.error());
  auto message = ipc::DecodeMessage(header);
  if (!message) return std::unexpected(message.error());
  if (message->header_type != ipc::HeaderType::kRecordBatch)
    return Fail("expected record batch, dictionaries unsupported");
  const auto &b = message->batch;
  if (body.size() > kMaxBody || message->body_length < 0 ||
      static_cast<uint64_t>(message->body_length) != body.size() || b.length < 0 ||
      static_cast<uint64_t>(b.length) > kMaxRows || b.nodes.size() != schema.fields.size())
    return Fail("body length, row count or node count mismatch");
  RecordBatch result{schema, {}, static_cast<size_t>(b.length)};
  if (result.row_count && (schema.fields.empty() || schema.fields.size() > kMaxCells / result.row_count))
    return Fail("batch logical cell count exceeds policy");
  size_t next = 0, budget = kMaxBody, previous_end = 0, next_variadic = 0, materialized = 0;
  auto buffer = [&](size_t limit = kMaxBody) -> FlightResult<DecodedBuffer> {
    if (next >= b.buffers.size()) return Fail("missing body buffer");
    const auto item = b.buffers[next++];
    if (item.offset < 0 || item.length < 0 || static_cast<uint64_t>(item.offset) > body.size() ||
        static_cast<uint64_t>(item.length) > body.size() - static_cast<size_t>(item.offset) ||
        static_cast<uint64_t>(item.offset) < previous_end || item.offset % 8 != 0)
      return Fail("invalid body buffer range");
    previous_end = static_cast<size_t>(item.offset + item.length);
    return Decompress(body.subspan(static_cast<size_t>(item.offset), static_cast<size_t>(item.length)), b.compression,
                      budget, limit);
  };
  for (size_t i = 0; i < schema.fields.size(); ++i) {
    const auto &f = schema.fields[i];
    auto layout = GetLayout(f);
    if (!layout) return std::unexpected(layout.error());
    const auto &l = *layout;
    const auto node = b.nodes[i];
    if (node.length != b.length || node.null_count < 0 || node.null_count > b.length) return Fail("invalid field node");
    auto semantic = Semantic(f);
    if (!semantic) return std::unexpected(semantic.error());
    Column c;
    c.set_column_name(f.name);
    c.set_datatype(static_cast<::greptime::v1::ColumnDataType>(l.datatype));
    c.set_semantic_type(static_cast<::greptime::v1::SemanticType>(*semantic));
    if (l.datatype == 30) {
      auto *ext = c.mutable_datatype_extension()->mutable_decimal_type();
      ext->set_precision(f.type.decimal_precision);
      ext->set_scale(f.type.scale);
    }
    if (l.datatype == 31)
      c.mutable_datatype_extension()->mutable_json_native_type()->set_datatype(::greptime::v1::BINARY);
    auto *values = c.mutable_values();
    size_t nulls = 0;
    if (f.type.id == ipc::TypeId::kNull) {
      if (node.null_count != b.length || !f.nullable) return Fail("invalid null field node");
      std::string mask((result.row_count + 7) / 8, '\0');
      for (size_t row = 0; row < result.row_count; ++row)
        mask[row / 8] |= static_cast<char>(1U << (row % 8));
      c.set_null_mask(std::move(mask));
      result.columns.push_back(std::move(c));
      continue;
    }
    auto validity = buffer(((result.row_count + 7) / 8 + 7) & ~size_t{7});
    if (!validity) return std::unexpected(validity.error());
    if (!validity->bytes.empty() && validity->bytes.size() < (result.row_count + 7) / 8)
      return Fail("short validity bitmap");
    if (node.null_count && validity->bytes.empty()) return Fail("missing validity bitmap for nulls");
    DecodedBuffer offsets;
    if (l.variable) {
      auto read = buffer(((result.row_count + 1) * l.width + 7) & ~size_t{7});
      if (!read) return std::unexpected(read.error());
      offsets = std::move(*read);
    }
    const size_t fixed_size = l.datatype == 0 ? (result.row_count + 7) / 8 : result.row_count * l.width;
    auto data = buffer(l.variable ? kMaxBody : (fixed_size + 7) & ~size_t{7});
    if (!data) return std::unexpected(data.error());
    std::vector<DecodedBuffer> variadic;
    if (l.view) {
      if (next_variadic >= b.variadic_buffer_counts.size()) return Fail("missing variadic buffer count");
      const auto count = b.variadic_buffer_counts[next_variadic++];
      if (count < 0 || static_cast<uint64_t>(count) > b.buffers.size() - next)
        return Fail("invalid variadic buffer count");
      for (int64_t j = 0; j < count; ++j) {
        auto payload = buffer();
        if (!payload) return std::unexpected(payload.error());
        variadic.push_back(std::move(*payload));
      }
    }
    if (l.variable) {
      if (offsets.bytes.size() < (result.row_count + 1) * l.width) return Fail("invalid offset buffer");
      uint64_t prior = 0;
      for (size_t row = 0; row <= result.row_count; ++row) {
        auto end = Read(offsets, row * l.width, l.width);
        if (uarch::unlikely(end < prior || end > data->bytes.size())) return Fail("non-monotone/out-of-range offset");
        prior = end;
      }
    } else if (data->bytes.size() < (l.datatype == 0 ? (result.row_count + 7) / 8 : result.row_count * l.width))
      return Fail("short fixed-width data buffer");
    std::string mask((result.row_count + 7) / 8, '\0');
    for (size_t row = 0; row < result.row_count; ++row) {
      const bool is_null = !validity->bytes.empty() && !((*validity)[row / 8] & (1U << (row % 8)));
      if (is_null) {
        ++nulls;
        mask[row / 8] |= static_cast<char>(1U << (row % 8));
        continue;
      }
      if (l.view) {
        auto value = detail::DecodeView(*data, row, variadic);
        if (!value) return std::unexpected(value.error());
        if (value->size() > kMaxBody - materialized) return Fail("view values exceed decoded size policy");
        materialized += value->size();
        if (f.type.id == ipc::TypeId::kUtf8View && !ValidUtf8(*value)) return Fail("invalid UTF-8 value");
        if (l.datatype == 12)
          values->add_string_values(std::string(*value));
        else
          values->add_binary_values(std::string(*value));
      } else if (l.variable) {
        const auto start = Read(offsets, row * l.width, l.width), end = Read(offsets, (row + 1) * l.width, l.width);
        const auto slice = data->bytes.subspan(start, end - start);
        if (uarch::unlikely(l.datatype == 12 && !ValidUtf8(slice))) return Fail("invalid UTF-8 value");
        std::string str(slice.begin(), slice.end());
        if (l.datatype == 12)
          values->add_string_values(std::move(str));
        else
          values->add_binary_values(std::move(str));
      } else if (l.datatype == 0)
        values->add_bool_values(((*data)[row / 8] & (1U << (row % 8))) != 0);
      else
        AppendDecoded(*values, l, *data, row * l.width);
    }
    if (nulls != static_cast<uint64_t>(node.null_count) || (!f.nullable && nulls))
      return Fail("null count/nullable mismatch");
    if (nulls) c.set_null_mask(std::move(mask));
    result.columns.push_back(std::move(c));
  }
  if (next != b.buffers.size()) return Fail("unexpected extra body buffers");
  if (next_variadic != b.variadic_buffer_counts.size()) return Fail("unexpected variadic buffer counts");
  return result;
}

FlightResult<FlightTimestampBounds> TimestampBounds(const RecordBatch &batch) {
  auto validated = ValidateBatch(batch);
  if (!validated) return std::unexpected(validated.error());
  if (!batch.row_count) return FlightTimestampBounds{};
  auto index = TimeColumn(batch);
  if (!index) return std::unexpected(index.error());
  if (*index >= batch.columns.size()) return Fail("timestamp column missing");
  auto layout = GetLayout(batch.schema.fields[*index]);
  if (!layout) return std::unexpected(layout.error());
  const auto &values = batch.columns[*index].values();
  const auto *r = values.GetReflection();
  int64_t low = std::numeric_limits<int64_t>::max(), high = std::numeric_limits<int64_t>::min();
  for (size_t row = 0; row < batch.row_count; ++row) {
    if (NullAt(batch.columns[*index], row)) return Fail("null timestamp");
    auto value = r->GetRepeatedInt64(values, ValueField(*layout), static_cast<int>(row));
    low = std::min(low, value);
    high = std::max(high, value);
  }
  return FlightTimestampBounds{low, high};
}
FlightResult<std::vector<RecordBatch>> PartitionRecordBatchByHour(const RecordBatch &batch) {
  auto bounds = TimestampBounds(batch);
  if (!bounds) return std::unexpected(bounds.error());
  if (!batch.row_count) return std::vector<RecordBatch>{batch};
  auto index = TimeColumn(batch);
  if (!index) return std::unexpected(index.error());
  const auto &f = batch.schema.fields[*index];
  auto layout = GetLayout(f);
  static constexpr std::array<uarch::FastDivisorU64, 4> kHourDivisors{
      uarch::divisor_u64<3600ULL>(), uarch::divisor_u64<3'600'000ULL>(), uarch::divisor_u64<3'600'000'000ULL>(),
      uarch::divisor_u64<3'600'000'000'000ULL>()};
  const auto &hour_div = kHourDivisors[static_cast<size_t>(f.type.unit)];
  const auto &tv = batch.columns[*index].values();
  const auto *tr = tv.GetReflection();
  struct HourRows {
    int64_t hour = 0;
    std::vector<size_t> rows;
  };
  std::vector<HourRows> groups;
  for (size_t row = 0; row < batch.row_count; ++row) {
    const int64_t value = tr->GetRepeatedInt64(tv, ValueField(*layout), static_cast<int>(row));
    int64_t bucket = 0;
    if (uarch::likely(value >= 0))
      bucket = static_cast<int64_t>(hour_div.div(static_cast<uint64_t>(value)));
    else {
      const uint64_t abs_val = 0ULL - static_cast<uint64_t>(value);
      const auto quotient = hour_div.div_rem(abs_val);
      bucket = -static_cast<int64_t>(quotient.first) - static_cast<int64_t>(quotient.second != 0U);
    }
    auto slot = std::ranges::lower_bound(groups, bucket, {}, &HourRows::hour);
    if (slot == groups.end() || slot->hour != bucket) {
      if (uarch::unlikely(groups.size() >= kMaxPartitions)) return Fail("hour partitions exceed policy");
      slot = groups.insert(slot, HourRows{bucket, {}});
    }
    slot->rows.push_back(row);
  }
  auto schema_wire = ipc::EncodeSchema(batch.schema);
  if (!schema_wire) return std::unexpected(schema_wire.error());
  const auto schema_bytes = uarch::checked_mul(schema_wire->size(), groups.size());
  if (!schema_bytes || *schema_bytes > kMaxBody) return Fail("partition schemas exceed size policy");
  std::vector<RecordBatch> result;
  for (const auto &group : groups) {
    const auto &rows = group.rows;
    RecordBatch part{batch.schema, {}, rows.size()};
    for (size_t col = 0; col < batch.columns.size(); ++col) {
      const auto &source = batch.columns[col];
      auto l = GetLayout(batch.schema.fields[col]);
      if (!l) return std::unexpected(l.error());
      Column target = source;
      target.clear_values();
      target.clear_null_mask();
      std::string mask((rows.size() + 7) / 8, '\0');
      size_t packed = 0, selected = 0;
      for (size_t row = 0; row < batch.row_count && selected < rows.size(); ++row) {
        const bool is_null = NullAt(source, row);
        if (row == rows[selected]) {
          if (is_null)
            mask[selected / 8] |= static_cast<char>(1U << (selected % 8));
          else if (l->variable || l->view) {
            const auto &str = l->datatype == 12 ? source.values().string_values(static_cast<int>(packed))
                                                : source.values().binary_values(static_cast<int>(packed));
            if (l->datatype == 12)
              target.mutable_values()->add_string_values(str);
            else
              target.mutable_values()->add_binary_values(str);
          } else if (l->datatype == 0)
            target.mutable_values()->add_bool_values(source.values().bool_values(static_cast<int>(packed)));
          else {
            Bytes native;
            auto appended = AppendNative(native, source.values(), *l, packed);
            if (!appended) return std::unexpected(appended.error());
            AppendDecoded(*target.mutable_values(), *l, native, 0);
          }
          ++selected;
        }
        packed += !is_null;
      }
      if (std::any_of(mask.begin(), mask.end(), [](char c) { return c != 0; })) target.set_null_mask(std::move(mask));
      part.columns.push_back(std::move(target));
    }
    result.push_back(std::move(part));
  }
  return result;
}
} // namespace greptimedb::flight
