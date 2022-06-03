//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  end_ = false;
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (end_) {
    return false;
  }
  Tuple source_tuple;
  RID source_rid;
  if (!child_executor_->Next(&source_tuple, &source_rid)) {
    end_ = true;
    return false;
  }
  if (exec_ctx_->GetTransaction()->IsSharedLocked(source_rid)) {
    exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), source_rid);
  } else if (!exec_ctx_->GetTransaction()->IsExclusiveLocked(source_rid)) {
    exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), source_rid);
  }
  Tuple updated_tuple = GenerateUpdatedTuple(source_tuple);
  if (!table_info_->table_->UpdateTuple(updated_tuple, source_rid, exec_ctx_->GetTransaction()) &&
      exec_ctx_->GetTransaction()->GetState() != TransactionState::ABORTED) {
    if (table_info_->table_->MarkDelete(source_rid, exec_ctx_->GetTransaction())) {
      // table_info_->table_->ApplyDelete(source_rid, exec_ctx_->GetTransaction());
    }
    table_info_->table_->InsertTuple(updated_tuple, &source_rid, exec_ctx_->GetTransaction());
  }
  for (auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    auto key =
        source_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->DeleteEntry(key, source_rid, exec_ctx_->GetTransaction());
    key = updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->InsertEntry(key, source_rid, exec_ctx_->GetTransaction());
    auto index_write_record = IndexWriteRecord(source_rid, table_info_->oid_, WType::UPDATE, updated_tuple,
                                               index_info->index_oid_, exec_ctx_->GetCatalog());
    index_write_record.old_tuple_ = source_tuple;
    exec_ctx_->GetTransaction()->GetIndexWriteSet()->push_back(index_write_record);
  }
  return true;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
