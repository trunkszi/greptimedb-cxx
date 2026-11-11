#pragma once
#include <greptimedb/batch/column_buf.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <ranges>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace column_buf_test {
using namespace greptimedb;
using Type = ColumnDataType;
using Values = greptime::v1::Column::Values;
constexpr auto kMax = std::numeric_limits<std::size_t>::max();

inline TypedColumnBuf Column(Type type) { return {"value", SemanticType::kField, type}; }
inline std::span<const std::byte> Bytes(std::string_view text) {
  return std::as_bytes(std::span{text.data(), text.size()});
}
inline std::string Mask(std::initializer_list<unsigned char> bytes) { return {bytes.begin(), bytes.end()}; }
template <typename Message> std::string Wire(const Message &message) { return message.SerializeAsString(); }

template <typename Writer> void AssertWire(Type type, ValueView value, int datatype, Writer writer) {
  auto buf = Column(type);
  ASSERT_TRUE(buf.Push(std::monostate{}));
  ASSERT_TRUE(buf.Push(value));
  ASSERT_TRUE(buf.Push(std::monostate{}));
  EXPECT_EQ(buf.RowCount(), 3U);
  EXPECT_EQ(buf.NonNullLen(), 1U);
  EXPECT_EQ(buf.Nulls().NullCount(), 2U);
  EXPECT_EQ(buf.Name(), "value");
  EXPECT_EQ(buf.SemanticTypeValue(), SemanticType::kField);
  EXPECT_EQ(buf.DataType(), type);
  greptime::v1::Column expected;
  expected.set_column_name("value");
  expected.set_semantic_type(greptime::v1::FIELD);
  expected.set_datatype(static_cast<greptime::v1::ColumnDataType>(datatype));
  expected.set_null_mask(Mask({5}));
  writer(*expected.mutable_values());
  if (type == Type::kJson) {
    expected.mutable_datatype_extension()->set_json_type(greptime::v1::JSON_BINARY);
  } else if (type.kind == Type::Kind::kDecimal128) {
    auto *extension = expected.mutable_datatype_extension()->mutable_decimal_type();
    extension->set_precision(type.precision);
    extension->set_scale(type.scale);
  }
  EXPECT_EQ(Wire(buf.ToProtoColumn()), Wire(expected));
}
} // namespace column_buf_test
