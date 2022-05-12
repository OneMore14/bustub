//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
  index_info_ = exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  if (plan_->IsRawInsert()) {
    raw_index_ = 0;
    raw_value_size_ = plan_->RawValues().size();
  } else {
    child_executor_->Init();
  }
  end_ = false;
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (end_) {
    return false;
  }
  if (plan_->IsRawInsert()) {
    auto raw_data = plan_->RawValuesAt(raw_index_);
    Tuple new_tuple(raw_data, &table_info_->schema_);
    RID new_rid;
    bool ok = table_info_->table_->InsertTuple(new_tuple, &new_rid, exec_ctx_->GetTransaction());
    for (auto &index_info : index_info_) {
      auto key =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, new_rid, exec_ctx_->GetTransaction());
    }
    raw_index_++;
    if (raw_index_ == raw_value_size_) {
      end_ = true;
    }
    return ok;
  }
  bool ok = false;
  RID new_rid;
  Tuple new_tuple;
  ok = child_executor_->Next(&new_tuple, &new_rid);
  if (!ok) {
    end_ = true;
    return false;
  }
  ok = table_info_->table_->InsertTuple(new_tuple, &new_rid, exec_ctx_->GetTransaction());
  for (auto &index_info : index_info_) {
    auto key = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->InsertEntry(key, new_rid, exec_ctx_->GetTransaction());
  }
  return ok;

  return false;
}

}  // namespace bustub
