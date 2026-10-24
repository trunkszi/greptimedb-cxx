/** Dynamic structure-of-arrays table batches and their wire encoders. */
#pragma once

#include <greptime/v1/column.pb.h>
#include <greptime/v1/database.pb.h>
#include <greptimedb/batch/batch_encode.h>
#include <greptimedb/batch/column_buf.h>
#include <greptimedb/core/buffer_policy.h>
#include <greptimedb/core/error.h>
#include <greptimedb/core/types.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace greptimedb {

/** Ordered columns sharing one GreptimeDB schema and table name.
 *
 * Appending validates the complete row against every column before mutating
 * any buffer. Schema changes are rejected once rows are buffered. Clearing
 * applies the construction policy so warmed allocations survive ingestion
 * cycles unless the caller supplies another policy explicitly.
 */
class ColumnarBatch {
public:
  explicit ColumnarBatch(std::string_view table);
  ColumnarBatch(std::string_view table, std::size_t row_capacity);
  ColumnarBatch(std::string_view table, std::size_t row_capacity,
                BufferPolicy policy);

  std::string_view TableName() const noexcept { return table_; }
  void SetTableName(std::string table) { table_ = std::move(table); }

  std::size_t RowCount() const noexcept;
  bool IsEmpty() const noexcept { return RowCount() == 0; }
  std::size_t ColumnCount() const noexcept { return columns_.size(); }
  std::span<const TypedColumnBuf> Columns() const noexcept { return columns_; }

  const TypedColumnBuf *Column(std::size_t index) const noexcept;
  const TypedColumnBuf *ColumnByName(std::string_view name) const noexcept;
  TypedColumnBuf *ColumnMut(std::size_t index) noexcept;
  TypedColumnBuf *ColumnByNameMut(std::string_view name) noexcept;

  Result<std::size_t> AddColumn(std::string_view name,
                                SemanticType semantic_type,
                                ColumnDataType data_type);
  Result<std::size_t> AddTagColumn(std::string_view name,
                                   ColumnDataType data_type);
  Result<std::size_t> AddTimestampColumn(std::string_view name,
                                         TimestampUnit unit);
  Result<std::size_t> AddFieldColumn(std::string_view name,
                                     ColumnDataType data_type);

  /** Swaps in a replacement column without checking the schema; returns the
   * previous column. */
  Result<TypedColumnBuf> ReplaceColumn(std::size_t index,
                                       TypedColumnBuf replacement);

  /** All columns commit together after fallible payload preparation. Failure
   * preserves logical rows and values, but may retain newly grown capacity. */
  Result<void> PushRow(std::span<const ValueView> values);
  Result<void> PushResultRow(std::span<const ValueView> values);

  Result<void> ValidateSchema() const;
  Result<void> ValidateResultShape() const;

  Result<::greptime::v1::InsertRequest> EncodeInsertRequest() const;
  Result<::greptime::v1::InsertRequest>
  EncodeInsertRequestFor(std::string_view table_name) const;
  Result<::greptime::v1::DeleteRequest> EncodeDeleteRequest() const;
  Result<::greptime::v1::DeleteRequest>
  EncodeDeleteRequestFor(std::string_view table_name) const;

  std::size_t AllocatedBytes() const noexcept;
  std::size_t RowCapacity() const noexcept;
  void Clear() noexcept { ClearWithPolicy(policy_); }
  void ClearWithPolicy(BufferPolicy policy) noexcept;
  void ClearUnbounded() noexcept { ClearWithPolicy(BufferPolicy::Unbounded()); }
  void ShrinkToFit() noexcept;

private:
  Result<void> AppendRow(std::span<const ValueView> values);

  std::string table_;
  std::vector<TypedColumnBuf> columns_;
  BufferPolicy policy_;
  std::size_t initial_row_capacity_ = 0;
};

static_assert(MemoryBufferConcept<ColumnarBatch>);

static_assert(BatchEncodeConcept<ColumnarBatch>);

/** Owned insert snapshots for one multi-table request. Adds are atomic: a
 * failed snapshot never changes the accumulated group. */
class MultiTableBatch {
public:
  MultiTableBatch() = default;
  MultiTableBatch(const MultiTableBatch &other);
  MultiTableBatch &operator=(const MultiTableBatch &other);
  MultiTableBatch(MultiTableBatch &&) noexcept = default;
  MultiTableBatch &operator=(MultiTableBatch &&) noexcept = default;
  template <BatchEncodeConcept Batch>
  Result<void> AddBatch(const Batch &batch) {
    auto request = batch.EncodeInsertRequest();
    if (!request)
      return std::unexpected(EncodeError(std::move(request.error())));
    return PushRequest(std::move(*request));
  }

  template <BatchEncodeConcept Batch>
  Result<void> AddBatchTo(std::string_view table_name, const Batch &batch) {
    if (table_name.find_first_not_of(" \t\n\r\f\v") == std::string_view::npos)
      return std::unexpected(
          Error::InvalidConfig("table name must not be empty"));
    auto request = [&] {
      if constexpr (requires { batch.EncodeInsertRequestFor(table_name); }) {
        return batch.EncodeInsertRequestFor(table_name);
      } else {
        auto encoded = batch.EncodeInsertRequest();
        if (encoded)
          encoded->set_table_name(table_name.data(), table_name.size());
        return encoded;
      }
    }();
    if (!request)
      return std::unexpected(EncodeError(std::move(request.error())));
    return PushRequest(std::move(*request));
  }

  std::size_t TotalRows() const noexcept;
  std::size_t TableCount() const noexcept;

  ::greptime::v1::InsertRequests IntoInsertRequests();
  ::greptime::v1::InsertRequests ToInsertRequests() const;
  void Clear() noexcept;

private:
  static Error EncodeError(Error error) { return error; }
  static Error EncodeError(ColumnBufError error);
  Result<void> PushRequest(::greptime::v1::InsertRequest request);

  detail::ColumnArray<::greptime::v1::InsertRequest> requests_;
};

} // namespace greptimedb
