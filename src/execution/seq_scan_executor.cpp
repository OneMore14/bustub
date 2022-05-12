//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  bpm_ = exec_ctx->GetBufferPoolManager();
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid());
  first_page_id_ = table_info_->table_->GetFirstPageId();
}

void SeqScanExecutor::Init() {
  auto first_page = static_cast<TablePage *>(bpm_->FetchPage(first_page_id_));
  end_ = !first_page->GetFirstTupleRid(&cur_rid_);
  bpm_->UnpinPage(first_page_id_, false, nullptr);
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple new_tuple;
  while (!end_) {
    if (!table_info_->table_->GetTuple(cur_rid_, &new_tuple, exec_ctx_->GetTransaction())) {
      return false;
    }

    page_id_t origin_page_id = cur_rid_.GetPageId();
    uint32_t origin_slot_num = cur_rid_.GetSlotNum();
    auto cur_page = bpm_->FetchPage(origin_page_id);
    assert(cur_page != nullptr);
    auto cur_table_page = static_cast<TablePage *>(cur_page);
    RID next_rid;
    if (!cur_table_page->GetNextTupleRid(cur_rid_, &next_rid)) {
      page_id_t next_page_id = cur_table_page->GetNextPageId();
      if (next_page_id == INVALID_PAGE_ID) {
        end_ = true;
      } else {
        auto next_page = bpm_->FetchPage(next_page_id);
        assert(next_page != nullptr);
        auto next_table_page = static_cast<TablePage *>(next_page);
        if (!next_table_page->GetFirstTupleRid(&next_rid)) {
          end_ = true;
        }
        bpm_->UnpinPage(next_page_id, false);
      }
    }
    cur_rid_.Set(next_rid.GetPageId(), next_rid.GetSlotNum());
    bpm_->UnpinPage(origin_page_id, false);

    std::vector<Value> values;
    for (auto &colum : plan_->OutputSchema()->GetColumns()) {
      values.push_back(colum.GetExpr()->Evaluate(&new_tuple, &table_info_->schema_));
    }
    new_tuple = Tuple(values, plan_->OutputSchema());
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(&new_tuple, plan_->OutputSchema()).GetAs<bool>()) {
      rid->Set(origin_page_id, origin_slot_num);

      *tuple = new_tuple;

      return true;
    }
  }
  return false;
}

}  // namespace bustub
