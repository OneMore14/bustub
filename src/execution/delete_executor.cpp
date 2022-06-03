//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  end_ = false;
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (end_) {
    return false;
  }
  Tuple delete_tuple;
  RID delete_rid;
  if (!child_executor_->Next(&delete_tuple, &delete_rid)) {
    end_ = true;
    return false;
  }
  if (exec_ctx_->GetTransaction()->IsSharedLocked(delete_rid)) {
    exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), delete_rid);
  } else if (!exec_ctx_->GetTransaction()->IsExclusiveLocked(delete_rid)) {
    exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), delete_rid);
  }
  if (table_info_->table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction())) {
    // table_info_->table_->ApplyDelete(delete_rid, exec_ctx_->GetTransaction());
  }

  for (auto &index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
    auto key =
        delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->DeleteEntry(key, delete_rid, exec_ctx_->GetTransaction());
    exec_ctx_->GetTransaction()->GetIndexWriteSet()->push_back(IndexWriteRecord(
        delete_rid, table_info_->oid_, WType::DELETE, delete_tuple, index_info->index_oid_, exec_ctx_->GetCatalog()));
  }

  return true;
}

}  // namespace bustub
