/** Single-table columnar encoding shared by dynamic and reflected batches. */
#pragma once

#include <greptime/v1/database.pb.h>

#include <concepts>
#include <cstddef>
#include <expected>
#include <string_view>
#include <type_traits>

namespace greptimedb {
namespace batch_detail {

/** Matches an expected result carrying Payload regardless of its error type. */
template <class T, class Payload>
concept ExpectedValue = requires {
  typename std::remove_cvref_t<T>::value_type;
  typename std::remove_cvref_t<T>::error_type;
} && std::same_as<typename std::remove_cvref_t<T>::value_type, Payload>;

} // namespace batch_detail

/** A columnar batch exposes the ordered table identity and both wire encoders. */
template <class T>
concept BatchEncodeConcept = requires(const T &batch) {
  { batch.TableName() } -> std::same_as<std::string_view>;
  { batch.RowCount() } -> std::same_as<std::size_t>;
  requires batch_detail::ExpectedValue<decltype(batch.EncodeInsertRequest()),
                                        ::greptime::v1::InsertRequest>;
  requires batch_detail::ExpectedValue<decltype(batch.EncodeDeleteRequest()),
                                        ::greptime::v1::DeleteRequest>;
};

} // namespace greptimedb
