//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "include/common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"
namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "index invalid");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index > 0 && index < GetSize(), "index invalid");
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Find(const KeyType &key, KeyComparator &comp) const -> ValueType {
  auto it = std::upper_bound(array_ + 1, array_ + GetSize(), key,
                             [&](const KeyType &k, const MappingType &kv) { return comp(k, kv.first) < 0; });
  return (it - 1)->second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &comp) {
  IncreaseSize(1);
  int k = GetSize() - 2;  // index of last key
  if (k < 1 || comp(key, array_[k].first) > 0) {
    array_[k + 1] = {key, value};
    return;
  }

  // leaf page wouldn't occur duplicated key
  for (; k > 0; k--) {
    if (comp(key, array_[k].first) < 0) {
      array_[k + 1] = array_[k];
    } else {
      array_[k + 1] = {key, value};
      return;
    }
  }
  // insert only occur while child split, always keep new child at right, thus don't have to update array_[0]
  array_[1] = {key, value};
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, int index) -> bool {
  for (int k = GetSize() - 1; k >= index; k--) {
    array_[k + 1] = array_[k];
  }
  array_[index] = {key, value};
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteEntry(const KeyType &key, KeyComparator &comp) {
  // delete key and pointer to page whose key' >= key
  int k = 1;
  for (; k < GetSize(); k++) {
    if (comp(key, array_[k].first) == 0) {
      break;
    }
  }
  if (k == GetSize()) {
    return;
  }
  for (; k < GetSize() - 1; k++) {
    array_[k] = array_[k + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteEntry(int index) {
  if (index >= GetSize()) {
    return;
  }
  for (int k = index; k < GetSize() - 1; k++) {
    array_[k] = array_[k + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetArray(const std::vector<MappingType> &array, int begin, int end) {
  SetSize(end - begin);
  for (int i = 0, j = begin; j < end; i++, j++) {
    array_[i] = array[j];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetArray(std::vector<MappingType> &array) {
  array.reserve(GetSize() + 1);
  std::copy(array_, array_ + GetSize(), std::back_inserter(array));
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  BUSTUB_ASSERT(false, "value not exist");
  return 0;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
