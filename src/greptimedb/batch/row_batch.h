/** Thread-affine row-oriented batches for the GreptimeDB row insert protocol.
 */
#pragma once

#include <greptime/v1/database.pb.h>
#include <greptime/v1/row.pb.h>
#include <greptimedb/batch/column_buf.h>
#include <greptimedb/core/buffer_policy.h>
#include <greptimedb/core/error.h>
#include <greptimedb/core/types.h>

#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace greptimedb {

/** Rows sharing one ordered GreptimeDB schema.
 *
 * Pushing validates the complete row before mutating storage, so a failed push
 * leaves every buffered row unchanged. Clearing discards rows while retaining
 * row slots and their nested payload allocations within the buffer policy's
 * watermarks. Fallible growth prepares an inactive owned row before committing
 * its count. Protobuf allocation occurs only when encoding an owned request.
 */
class RowBatch {
public:
  explicit RowBatch(std::string_view table_name);
  RowBatch(std::string_view table_name, std::size_t row_capacity);
  RowBatch(std::string_view table_name, std::size_t row_capacity,
           BufferPolicy policy);

  RowBatch(const RowBatch &) = delete;
  RowBatch &operator=(const RowBatch &) = delete;
  RowBatch(RowBatch &&other) noexcept;
  RowBatch &operator=(RowBatch &&other) noexcept;

  [[nodiscard]] const std::string &TableName() const noexcept {
    return table_name_;
  }
  void SetTableName(std::string table_name) noexcept {
    table_name_ = std::move(table_name);
  }

  /** Appends a column definition and returns its ordered row position. */
  [[nodiscard]] Result<std::size_t> AddColumnSchema(std::string_view name,
                                                    SemanticType semantic_type,
                                                    ColumnDataType data_type);
  [[nodiscard]] Result<std::size_t> AddTagColumn(std::string_view name,
                                                 ColumnDataType data_type);
  [[nodiscard]] Result<std::size_t> AddTimestampColumn(std::string_view name,
                                                       TimestampUnit unit);
  [[nodiscard]] Result<std::size_t> AddFieldColumn(std::string_view name,
                                                   ColumnDataType data_type);

  /** Appends one complete row after checking every value against the schema. */
  [[nodiscard]] Result<void> PushRow(std::span<const ValueView> values);
  /** Checks the time index and the width and non-nullity of buffered rows. */
  [[nodiscard]] Result<void> ValidateSchema() const;

  [[nodiscard]] std::size_t RowCount() const noexcept { return row_count_; }
  [[nodiscard]] bool IsEmpty() const noexcept { return row_count_ == 0; }
  [[nodiscard]] std::size_t ColumnCount() const noexcept {
    return columns_.Size();
  }

  [[nodiscard]] Result<::greptime::v1::RowInsertRequest>
  EncodeRowInsertRequest() const;
  [[nodiscard]] Result<::greptime::v1::RowInsertRequest>
  EncodeRowInsertRequestFor(std::string_view table_name) const;
  [[nodiscard]] Result<::greptime::v1::RowDeleteRequest>
  EncodeRowDeleteRequest() const;
  [[nodiscard]] Result<::greptime::v1::RowDeleteRequest>
  EncodeRowDeleteRequestFor(std::string_view table_name) const;

  [[nodiscard]] std::size_t AllocatedBytes() const noexcept;
  [[nodiscard]] std::size_t RowCapacity() const noexcept;
  void Clear() noexcept;
  void ClearWithPolicy(BufferPolicy policy) noexcept;
  void ClearUnbounded() noexcept;
  void ShrinkToFit() noexcept;

private:
  struct StoredValue {
    ValueView value;
    detail::ColumnArray<std::byte> payload;
    bool Prepare(const ValueView &input) noexcept;
    void Commit(const ValueView &input) noexcept;
  };
  using StoredRow = detail::ColumnArray<StoredValue>;
  struct Column {
    detail::ColumnArray<char> name;
    SemanticType semantic_type = SemanticType::kField;
    ColumnDataType data_type;
    std::string_view Name() const noexcept {
      return {name.Data(), name.Size()};
    }
  };
  auto Columns() const noexcept {
    return std::span<const Column>(columns_.Data(), columns_.Size());
  }

  [[nodiscard]] Result<void>
  ValidateDestination(std::string_view table_name) const;
  [[nodiscard]] static Result<void> ValidateValue(const Column &column,
                                                  const ValueView &value);

  std::string table_name_;
  detail::ColumnArray<Column> columns_;
  detail::ColumnArray<StoredRow> rows_;
  std::size_t row_count_ = 0;
  BufferPolicy policy_;
};

static_assert(MemoryBufferConcept<RowBatch>);

} // namespace greptimedb
