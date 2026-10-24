#pragma once

#include <greptimedb/batch/batch_encode.h>
#include <greptimedb/row/reflected_column.h>

#include <array>
#include <limits>
#include <ranges>
#include <tuple>
#include <utility>

namespace greptimedb {
namespace row_detail {

template <class T, std::size_t I>
using MemberType = [:std::meta::type_of(RowSchema<T>::kMembers[I]):];

template <class T, std::size_t... I>
auto BorrowTuple(std::index_sequence<I...>)
    -> std::tuple<Borrowed<MemberType<T, I>>...>;
template <class T, std::size_t... I>
auto ColumnTuple(std::index_sequence<I...>)
    -> std::tuple<ReflectedColumnBuf<MemberType<T, I>>...>;
template <class T>
using Indices = std::make_index_sequence<RowSchema<T>::kColumns.size()>;
template <class T>
using BorrowTupleType = decltype(BorrowTuple<T>(Indices<T>{}));
template <class T>
using ColumnTupleType = decltype(ColumnTuple<T>(Indices<T>{}));

template <class T> consteval bool SafeBorrowArgument() {
  using Arg = std::remove_cvref_t<T>;
  if constexpr (std::is_lvalue_reference_v<T>)
    return true;
  else if constexpr (OptionalTraits<Arg>::kOptional)
    return SafeBorrowArgument<typename OptionalTraits<Arg>::Type &&>();
  else if constexpr (std::ranges::range<Arg>)
    return std::ranges::borrowed_range<Arg>;
  else
    return std::is_scalar_v<Arg> || std::same_as<Arg, std::nullopt_t> ||
           ScalarDataType<Arg>().has_value();
}

template <class T, std::size_t... I>
BorrowTupleType<T> BorrowMembers(const T &row,
                                 std::index_sequence<I...>) noexcept {
  return {Borrow(row.[:RowSchema<T>::kMembers[I]:])...};
}
template <class T, std::size_t... I>
ColumnTupleType<T> MakeColumns(std::index_sequence<I...>) noexcept {
  return ColumnTupleType<T>{
      ReflectedColumnBuf<MemberType<T, I>>{RowSchema<T>::kColumns[I]}...};
}
template <class T, AnnotationText Name> consteval std::size_t ColumnIndex() {
  constexpr auto columns = RowSchema<T>::kColumns;
  for (std::size_t i = 0; i < columns.size(); ++i)
    if (columns[i].name == Name.View())
      return i;
  return columns.size();
}

inline const void *CallerThreadToken() noexcept {
  static thread_local const char token{};
  return &token;
}

} // namespace row_detail

/** Allocation-free input projection. Referenced strings and binary buffers must
 * remain alive through PushRef; the batch copies payloads into its own arenas.
 */
template <RowConcept T> class BorrowedRow {
public:
  using Tuple = row_detail::BorrowTupleType<T>;
  static constexpr std::size_t kColumnCount =
      row_detail::RowSchema<T>::kColumns.size();
  explicit BorrowedRow(const T &row) noexcept
      : values_(row_detail::BorrowMembers(row, row_detail::Indices<T>{})) {}
  BorrowedRow(T &&) = delete;
  BorrowedRow(const T &&) = delete;
  template <class U>
    requires std::same_as<std::remove_cvref_t<U>, Tuple>
  explicit BorrowedRow(U &&values) noexcept
      : values_(std::forward<U>(values)) {}
  template <class... U>
    requires(sizeof...(U) == kColumnCount &&
             std::constructible_from<Tuple, U...> &&
             (row_detail::SafeBorrowArgument<U &&>() && ...))
  explicit BorrowedRow(U &&...values) noexcept
      : values_(std::forward<U>(values)...) {}

  template <std::size_t I> const auto &Get() const & noexcept {
    return std::get<I>(values_);
  }
  template <std::size_t I> auto Get() const && noexcept {
    return std::get<I>(values_);
  }
  template <AnnotationText Name> const auto &Get() const & noexcept {
    constexpr auto index = row_detail::ColumnIndex<T, Name>();
    static_assert(index < kColumnCount, "unknown reflected column name");
    return Get<index>();
  }
  template <AnnotationText Name> auto Get() const && noexcept {
    constexpr auto index = row_detail::ColumnIndex<T, Name>();
    static_assert(index < kColumnCount, "unknown reflected column name");
    return std::get<index>(values_);
  }
  const Tuple &Values() const & noexcept { return values_; }
  Tuple Values() const && noexcept { return values_; }

private:
  Tuple values_;
};

/** Scalar values are projected by value; string and binary payloads borrow
 * their source. Any batch mutation attempt, including failed growth, may
 * invalidate projections. The source must outlive every payload view. */
template <RowConcept T> class RowView {
public:
  explicit RowView(BorrowedRow<T> row) noexcept : row_(std::move(row)) {}
  explicit RowView(const T &row) noexcept : row_(row) {}
  RowView(T &&) = delete;
  RowView(const T &&) = delete;
  template <std::size_t I> const auto &Get() const & noexcept {
    return row_.template Get<I>();
  }
  template <std::size_t I> auto Get() const && noexcept {
    return std::move(row_).template Get<I>();
  }
  template <AnnotationText Name> const auto &Get() const & noexcept {
    return row_.template Get<Name>();
  }
  template <AnnotationText Name> auto Get() const && noexcept {
    return std::move(row_).template Get<Name>();
  }

private:
  BorrowedRow<T> row_;
};

/** Reflected SoA ingestion buffers with compact non-null values. Reservation is
 * fallible; PushRef reuses reserved rows and per-column payload bytes. All
 * column appends are prepared before any logical row is committed. Payload
 * input that aliases this batch is rejected; use an independent source when
 * appending. */
template <RowConcept T> class TypedColumnarBatch {
public:
  static constexpr auto kColumns = row_detail::RowSchema<T>::kColumns;
  static constexpr std::size_t kColumnCount = kColumns.size();
  using Columns = row_detail::ColumnTupleType<T>;
  explicit TypedColumnarBatch(std::size_t rows = 0,
                              BufferPolicy policy = {}) noexcept
      : columns_(row_detail::MakeColumns<T>(row_detail::Indices<T>{})),
        policy_(policy) {
    // Constructor capacity is a hint; ReserveRows exposes allocation failure.
    const auto reserved = ReserveRows(rows);
    if (!reserved)
      ClearWithPolicy(BufferPolicy::Zero());
  }
  explicit TypedColumnarBatch(BufferPolicy policy) noexcept
      : TypedColumnarBatch(0, policy) {}
  ~TypedColumnarBatch() { CheckThread(); }
  TypedColumnarBatch(const TypedColumnarBatch &) = delete;
  TypedColumnarBatch &operator=(const TypedColumnarBatch &) = delete;
  TypedColumnarBatch(TypedColumnarBatch &&) = delete;
  TypedColumnarBatch &operator=(TypedColumnarBatch &&) = delete;

  static constexpr std::string_view TableName() noexcept {
    return row_detail::RowSchema<T>::kTableName;
  }
  std::size_t RowCount() const noexcept {
    CheckThread();
    return row_count_;
  }
  template <std::size_t I> const auto &Column() const & noexcept {
    CheckThread();
    return std::get<I>(columns_);
  }
  template <std::size_t I> const auto &Column() const && = delete;
  std::expected<void, ColumnBufError> ReserveRows(std::size_t rows) noexcept {
    CheckThread();
    return ForEachFallible(
        [rows](auto &column) { return column.ReserveRows(rows); });
  }
  /** Reserves this many payload bytes independently in every variable column.
   */
  std::expected<void, ColumnBufError> ReserveBytes(std::size_t bytes) noexcept {
    CheckThread();
    return ForEachFallible(
        [bytes](auto &column) { return column.ReserveBytes(bytes); });
  }
  template <std::size_t I>
  std::expected<void, ColumnBufError> ReserveBytes(std::size_t bytes) noexcept {
    CheckThread();
    return std::get<I>(columns_).ReserveBytes(bytes);
  }
  std::expected<void, ColumnBufError> Push(const T &row) noexcept {
    return PushRef(BorrowedRow<T>{row});
  }
  std::expected<void, ColumnBufError> PushRef(const T &row) noexcept {
    return Push(row);
  }
  std::expected<void, ColumnBufError>
  PushRef(const BorrowedRow<T> &row) noexcept {
    CheckThread();
    if (row_count_ >= std::numeric_limits<std::uint32_t>::max())
      return std::unexpected(ColumnBufError::kAllocationFailure);
    return Append(row, row_detail::Indices<T>{});
  }
  std::expected<RowView<T>, ColumnBufError>
  GetRow(std::size_t row) const & noexcept {
    CheckThread();
    if (row >= row_count_)
      return std::unexpected(ColumnBufError::kInvalidSchema);
    return Project(row, row_detail::Indices<T>{});
  }
  std::expected<RowView<T>, ColumnBufError>
      GetRow(std::size_t) const && = delete;
  /** Allocation-free row projections over the current row count. The batch must
   * outlive the range and must not be mutated while the range is used. */
  auto Views() const & noexcept {
    CheckThread();
    return std::views::iota(std::size_t{0}, row_count_) |
           std::views::transform([this](std::size_t row) {
             CheckThread();
             if (row >= row_count_)
               std::abort();
             return Project(row, row_detail::Indices<T>{});
           });
  }
  auto Views() const && = delete;
  std::expected<greptime::v1::InsertRequest, ColumnBufError>
  ToInsertRequest() const {
    CheckThread();
    greptime::v1::InsertRequest request;
    request.set_table_name(TableName().data(), TableName().size());
    request.set_row_count(static_cast<std::uint32_t>(row_count_));
    ForEach([&request](const auto &column) {
      row_detail::EncodeColumn(*request.add_columns(), column);
    });
    return request;
  }
  std::expected<greptime::v1::DeleteRequest, ColumnBufError>
  ToDeleteRequest() const {
    CheckThread();
    greptime::v1::DeleteRequest request;
    request.set_table_name(TableName().data(), TableName().size());
    request.set_row_count(static_cast<std::uint32_t>(row_count_));
    ForEach([&request](const auto &column) {
      if (column.Metadata().semantic_type != SemanticType::kField)
        row_detail::EncodeColumn(*request.add_key_columns(), column);
    });
    return request;
  }
  auto EncodeInsert() const { return ToInsertRequest(); }
  auto EncodeDelete() const { return ToDeleteRequest(); }
  auto EncodeInsertRequest() const { return ToInsertRequest(); }
  auto EncodeDeleteRequest() const { return ToDeleteRequest(); }

  std::size_t AllocatedBytes() const noexcept {
    CheckThread();
    std::size_t bytes = 0;
    ForEach([&bytes](const auto &column) {
      bytes = detail::SaturatingAdd(bytes, column.AllocatedBytes());
    });
    return bytes;
  }
  std::size_t RowCapacity() const noexcept {
    CheckThread();
    std::size_t rows = 0;
    ForEach([&rows](const auto &column) {
      rows = std::max(rows, column.RowCapacity());
    });
    return rows;
  }
  void Clear() noexcept { ClearWithPolicy(policy_); }
  void ClearUnbounded() noexcept { ClearWithPolicy(BufferPolicy::Unbounded()); }
  void ClearWithPolicy(BufferPolicy policy) noexcept {
    CheckThread();
    const bool release = policy.max_retained_rows == 0 ||
                         policy.max_retained_bytes == 0 ||
                         policy.ShouldShrink(RowCapacity(), AllocatedBytes());
    ForEach([release](auto &column) {
      column.ClearWithPolicy(release ? BufferPolicy::Zero()
                                     : BufferPolicy::Unbounded());
    });
    row_count_ = 0;
  }
  void ShrinkToFit() noexcept {
    CheckThread();
    ForEach([](auto &column) { column.ShrinkToFit(); });
  }
  BufferPolicy RetentionPolicy() const noexcept {
    CheckThread();
    return policy_;
  }
  void SetBufferPolicy(BufferPolicy policy) noexcept {
    CheckThread();
    policy_ = policy;
  }

private:
  void CheckThread() const noexcept {
    if (owner_ != row_detail::CallerThreadToken())
      std::abort();
  }
  template <class F> void ForEach(F &&function) noexcept {
    std::apply([&function](auto &...columns) { (function(columns), ...); },
               columns_);
  }
  template <class F> void ForEach(F &&function) const {
    std::apply(
        [&function](const auto &...columns) { (function(columns), ...); },
        columns_);
  }
  template <class F>
  std::expected<void, ColumnBufError> ForEachFallible(F &&function) noexcept {
    std::expected<void, ColumnBufError> result;
    ForEach([&result, &function](auto &column) {
      if (result)
        result = function(column);
    });
    return result;
  }
  template <std::size_t... I>
  std::expected<void, ColumnBufError>
  Append(const BorrowedRow<T> &row, std::index_sequence<I...>) noexcept {
    bool aliases = false;
    (
        [&] {
          const auto bytes =
              std::get<I>(columns_).InputBytes(row.template Get<I>());
          ForEach([&](const auto &column) {
            aliases = aliases || column.OwnsPayload(bytes);
          });
        }(),
        ...);
    if (aliases)
      return std::unexpected(ColumnBufError::kInvalidSchema);
    std::expected<void, ColumnBufError> ready;
    (
        [&] {
          if (ready)
            ready = std::get<I>(columns_).Prepare(row.template Get<I>());
        }(),
        ...);
    if (!ready)
      return ready;
    (std::get<I>(columns_).Commit(row.template Get<I>()), ...);
    ++row_count_;
    return {};
  }
  template <std::size_t... I>
  RowView<T> Project(std::size_t row,
                     std::index_sequence<I...>) const noexcept {
    return RowView<T>{BorrowedRow<T>{typename BorrowedRow<T>::Tuple{
        std::get<I>(columns_).BorrowAt(row)...}}};
  }
  Columns columns_;
  BufferPolicy policy_;
  std::size_t row_count_{};
  const void *owner_ = row_detail::CallerThreadToken();
};

} // namespace greptimedb
