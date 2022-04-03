//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  capacity_ = num_pages;
  size_ = 0;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (size_ <= 0) {
    return false;
  }
  *frame_id = l_.back();
  m_.erase(l_.back());
  l_.pop_back();
  size_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (m_.find(frame_id) == m_.end()) {
    return;
  }
  l_.erase(m_[frame_id]);
  m_.erase(frame_id);
  size_--;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (m_.find(frame_id) != m_.end() || size_ >= capacity_) {
    return;
  }

  l_.push_front(frame_id);
  m_[frame_id] = l_.begin();
  size_++;
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch_);
  return size_;
}

}  // namespace bustub
