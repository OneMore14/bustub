//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  Page *directory_page = buffer_pool_manager->NewPage(&directory_page_id_);
  assert(directory_page != nullptr);
  auto directory = reinterpret_cast<HashTableDirectoryPage *>(directory_page->GetData());
  directory->SetPageId(directory_page_id_);

  page_id_t bucket_page0_id;
  assert(buffer_pool_manager->NewPage(&bucket_page0_id) != nullptr);
  directory->SetBucketPageId(0, bucket_page0_id);
  directory->SetLocalDepth(0, 0);

  assert(buffer_pool_manager->UnpinPage(directory_page_id_, true));
  assert(buffer_pool_manager->UnpinPage(bucket_page0_id, false));
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetGlobalDepthMask() & Hash(key);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *directory_page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(directory_page != nullptr);
  return reinterpret_cast<HashTableDirectoryPage *>(directory_page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(bucket_page != nullptr);
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket = FetchBucketPage(bucket_page_id);
  bool found = bucket->GetValue(key, comparator_, result);

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));

  table_latch_.RUnlock();
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket = FetchBucketPage(bucket_page_id);
  bool ok;
  if (bucket->IsFull()) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    ok = SplitInsert(transaction, key, value);
    table_latch_.WUnlock();
    return ok;
  }

  ok = bucket->Insert(key, value, comparator_);

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, ok));
  table_latch_.WUnlock();
  return ok;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir = FetchDirectoryPage();
  auto origin_bucket_idx = KeyToDirectoryIndex(key, dir);
  page_id_t origin_bucket_page_id = KeyToPageId(key, dir);
  auto origin_bucket = FetchBucketPage(origin_bucket_page_id);
  auto origin_mask = dir->GetLocalDepthMask(origin_bucket_idx);
  auto origin_bucket_depth = dir->GetLocalDepth(origin_bucket_idx);
  auto new_mask = (origin_mask << 1) + 1;

  page_id_t new_bucket_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
  assert(new_page != nullptr);
  auto new_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());

  if (origin_bucket_depth == dir->GetGlobalDepth()) {
    for (uint32_t i = 0; i < dir->Size(); i++) {
      dir->SetBucketPageId(i + dir->Size(), dir->GetBucketPageId(i));
      dir->SetLocalDepth(i + dir->Size(), dir->GetLocalDepth(i));
    }
    dir->IncrGlobalDepth();
  }

  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (origin_bucket->IsReadable(i)) {
      KeyType origin_key = origin_bucket->KeyAt(i);
      if ((Hash(origin_key) & origin_mask) != (Hash(origin_key) & new_mask)) {
        assert(new_bucket->Insert(origin_key, origin_bucket->ValueAt(i), comparator_));
        origin_bucket->RemoveAt(i);
      }
    }
  }

  for (uint32_t i = 0; i < dir->Size(); i++) {
    if (dir->GetBucketPageId(i) == origin_bucket_page_id) {
      dir->IncrLocalDepth(i);
      if ((i & origin_mask) != (i & new_mask)) {
        dir->SetBucketPageId(i, new_bucket_page_id);
      }
    }
  }

  page_id_t insert_page_id = KeyToPageId(key, dir);
  bool ok = insert_page_id == origin_bucket_page_id ? origin_bucket->Insert(key, value, comparator_)
                                                    : new_bucket->Insert(key, value, comparator_);
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  assert(buffer_pool_manager_->UnpinPage(origin_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(new_bucket_page_id, true));

  return ok;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir);
  auto bucket = FetchBucketPage(bucket_page_id);
  auto ok = bucket->Remove(key, value, comparator_);

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, ok));

  if (bucket->IsEmpty()) {
    Merge(transaction, key, value);
    table_latch_.WUnlock();
    return ok;
  }

  table_latch_.WUnlock();
  return ok;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir);
  page_id_t bucket_page_id = KeyToPageId(key, dir);
  auto bucket_local_depth = dir->GetLocalDepth(bucket_idx);
  if (bucket_local_depth == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }
  auto pair_idx = dir->GetSplitImageIndex(bucket_idx);
  auto pair_page_id = dir->GetBucketPageId(pair_idx);
  if (bucket_local_depth != dir->GetLocalDepth(pair_idx)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }

  for (uint32_t i = 0; i < dir->Size(); i++) {
    if (dir->GetBucketPageId(i) == bucket_page_id) {
      dir->SetBucketPageId(i, pair_page_id);
      dir->DecrLocalDepth(i);
    } else if (dir->GetBucketPageId(i) == pair_page_id) {
      dir->DecrLocalDepth(i);
    }
  }

  while (dir->CanShrink()) {
    dir->DecrGlobalDepth();
  }

  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  assert(buffer_pool_manager_->DeletePage(bucket_page_id));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
