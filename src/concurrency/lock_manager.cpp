//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }
  auto &q = lock_table_[rid];
  q.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  bool flag = false;
  bool found_conflict = false;
start:
  while (true) {
    if (q.upgrading_ != INVALID_TXN_ID) {
      if (q.upgrading_ > txn->GetTransactionId()) {
        auto upgrade = TransactionManager::GetTransaction(q.upgrading_);
        ReleaseConflictLock(upgrade, rid);
        found_conflict = true;
        goto start;
      }
    }
    for (auto &request : q.request_queue_) {
      if (request.txn_id_ == txn->GetTransactionId()) {
        break;
      }
      if (request.txn_id_ > txn->GetTransactionId() && request.lock_mode_ == LockMode::EXCLUSIVE) {
        auto conflict_txn = TransactionManager::GetTransaction(request.txn_id_);
        ReleaseConflictLock(conflict_txn, rid);
        found_conflict = true;
        goto start;
      }
    }
    if (found_conflict) {
      q.cv_.notify_all();
      found_conflict = false;
    }
    if (q.upgrading_ == INVALID_TXN_ID) {
      for (auto &request : q.request_queue_) {
        if (request.txn_id_ == txn->GetTransactionId()) {
          flag = true;
          request.granted_ = true;
          break;
        }
        if (request.lock_mode_ != LockMode::SHARED) {
          break;
        }
      }
    }
    if (flag) {
      break;
    }
    q.cv_.wait(lk);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  auto &q = lock_table_[rid];
  q.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  bool flag = false;
  bool found_conflict = false;
start:
  while (true) {
    if (q.upgrading_ != INVALID_TXN_ID) {
      if (q.upgrading_ > txn->GetTransactionId()) {
        auto upgrade = TransactionManager::GetTransaction(q.upgrading_);
        ReleaseConflictLock(upgrade, rid);
        found_conflict = true;
        goto start;
      }
    }
    for (auto &request : q.request_queue_) {
      if (request.txn_id_ == txn->GetTransactionId()) {
        break;
      }
      if (request.txn_id_ > txn->GetTransactionId()) {
        auto conflict_txn = TransactionManager::GetTransaction(request.txn_id_);
        ReleaseConflictLock(conflict_txn, rid);
        found_conflict = true;
        goto start;
      }
    }
    if (found_conflict) {
      q.cv_.notify_all();
      found_conflict = false;
    }
    if (q.upgrading_ == INVALID_TXN_ID) {
      if (q.request_queue_.front().txn_id_ == txn->GetTransactionId()) {
        flag = true;
        q.request_queue_.front().granted_ = true;
      }
    }
    if (flag) {
      break;
    }
    q.cv_.wait(lk);
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto &q = lock_table_[rid];
  if (q.upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  q.upgrading_ = txn->GetTransactionId();
  bool flag = false;
  bool found_conflict = false;
start:
  while (true) {
    for (auto &request : q.request_queue_) {
      if (request.txn_id_ > txn->GetTransactionId() && request.granted_) {
        auto conflict_txn = TransactionManager::GetTransaction(request.txn_id_);
        ReleaseConflictLock(conflict_txn, rid);
        found_conflict = true;
        goto start;
      }
    }
    if (found_conflict) {
      q.cv_.notify_all();
      found_conflict = false;
    }
    if (q.request_queue_.front().txn_id_ == txn->GetTransactionId()) {
      int grant_count = 0;
      for (auto &request : q.request_queue_) {
        if (request.granted_) {
          grant_count++;
        }
      }
      if (grant_count == 1) {
        flag = true;
        q.request_queue_.front().lock_mode_ = LockMode::EXCLUSIVE;
        q.upgrading_ = INVALID_TXN_ID;
      }
    }
    if (flag) {
      break;
    }
    q.cv_.wait(lk);
    if (txn->GetState() == TransactionState::ABORTED) {
      q.upgrading_ = INVALID_TXN_ID;
      return false;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);
  bool found = false;
  auto &q = lock_table_[rid];
  for (auto it = q.request_queue_.begin(); it != q.request_queue_.end(); it++) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      if (it->granted_) {
        found = true;
        if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ ||
            (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && it->lock_mode_ == LockMode::EXCLUSIVE)) {
          if (txn->GetState() == TransactionState::GROWING) {
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      q.request_queue_.erase(it);
      break;
    }
  }
  if (!found) {
    return false;
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  q.cv_.notify_all();
  return true;
}

void LockManager::ReleaseConflictLock(Transaction *txn, const RID &rid) {
  auto &q = lock_table_[rid];
  if (q.upgrading_ == txn->GetTransactionId()) {
    q.upgrading_ = INVALID_TXN_ID;
  }

  auto it = q.request_queue_.begin();
  while (it != q.request_queue_.end()) {
    if (it->txn_id_ == txn->GetTransactionId()) {
      q.request_queue_.erase(it);
      break;
    }
    it++;
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  txn->SetState(TransactionState::ABORTED);
}

}  // namespace bustub
