//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k), evictable_frames_(*this) {
  BUSTUB_ASSERT(k > 1, "k should bigger than 1");
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  evictable_frames_.Pop(frame_id);
  node_store_.erase(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  BUSTUB_ASSERT(frame_id < (int32_t)replacer_size_, "Invalid: frame id larger than replacer_size_");
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {  // Not in buffer
    node_store_[frame_id] = LRUKNode(frame_id, k_, current_timestamp_++);
  } else {  // Already in buffer
    node_store_[frame_id].Access(current_timestamp_++);
    if (node_store_[frame_id].IsEvictable()) {
      evictable_frames_.Remove(frame_id);
      evictable_frames_.Insert(frame_id);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < (int32_t)replacer_size_, "Invalid: frame id larger than replacer_size_");
  std::lock_guard<std::mutex> lock(latch_);
  if (set_evictable && !node_store_[frame_id].IsEvictable()) {
    evictable_frames_.Insert(frame_id);
  } else if (!set_evictable && node_store_[frame_id].IsEvictable()) {
    evictable_frames_.Remove(frame_id);
  }
  node_store_[frame_id].SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }
  BUSTUB_ASSERT(node_store_[frame_id].IsEvictable(), "frame is not evictable");
  evictable_frames_.Remove(frame_id);
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

void LRUKReplacer::LRUKHeap::Heapify(int start, int end) {
  int fa = start;
  int son = fa * 2 + 1;
  while (son <= end) {
    if (son + 1 <= end && replacer_.node_store_[frames_[son]] < replacer_.node_store_[frames_[son + 1]]) {
      son++;
    }
    if (replacer_.node_store_[frames_[son]] < replacer_.node_store_[frames_[fa]]) {
      return;
    }
    std::swap(frames_[fa], frames_[son]);
    fa = son;
    son = fa * 2 + 1;
  }
}

void LRUKReplacer::LRUKHeap::Insert(frame_id_t frame_id) {
  frames_.push_back(frame_id);
  replacer_.curr_size_++;
  int son = replacer_.curr_size_ - 1;
  int fa = (son - 1) / 2;
  while (son > 0) {
    if (replacer_.node_store_[frames_[son]] < replacer_.node_store_[frames_[fa]]) {
      return;
    }
    std::swap(frames_[fa], frames_[son]);
    son = fa;
    fa = (son - 1) / 2;
  }
}

void LRUKReplacer::LRUKHeap::Remove(frame_id_t frame_id) {
  for (size_t i = 0; i < replacer_.curr_size_; i++) {
    if (frames_[i] == frame_id) {
      std::swap(frames_[i], frames_[replacer_.curr_size_ - 1]);
      frames_.pop_back();
      replacer_.curr_size_--;
      Heapify(i, replacer_.curr_size_ - 1);
      return;
    }
  }
}

void LRUKReplacer::LRUKHeap::Pop(frame_id_t *frame_id) {
  *frame_id = frames_[0];
  std::swap(frames_[0], frames_[replacer_.curr_size_ - 1]);
  frames_.pop_back();
  replacer_.curr_size_--;
  Heapify(0, replacer_.curr_size_ - 1);
}

}  // namespace bustub
