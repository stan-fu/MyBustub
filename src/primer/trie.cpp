#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

using std::string_view, std::shared_ptr, std::make_shared;

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (!root_) {
    return nullptr;
  }

  shared_ptr<TrieNode> cur = root_;
  for (char c : key) {
    if (cur->children_.count(c) == 0) {
      return nullptr;
    }
    cur = cur->children_.at(c);
  }
  if (!cur->is_value_node_) {
    return nullptr;
  }
  shared_ptr<TrieNodeWithValue<T>> key_node = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(cur);
  if (key_node == nullptr) {
    return nullptr;
  }
  return key_node->value_ == nullptr ? nullptr : key_node->value_.get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  if (key.empty()) {
    if (!root_->is_value_node_) {
      root_ = make_shared<TrieNodeWithValue<T>>(root_->children_, make_shared<T>(std::move(value)));
      return *this;
    }
    auto new_root = make_shared<TrieNodeWithValue<T>>(root_->children_, make_shared<T>(std::move(value)));
    return Trie(new_root);
  }

  shared_ptr<TrieNode> node = root_;
  int klen = key.size();
  for (int i = 0; i < klen - 1; i++) {
    if (node->children_.count(key[i]) == 0) {
      shared_ptr<TrieNode> tmp = std::const_pointer_cast<TrieNode>(node);
      tmp->children_.emplace(key[i], make_shared<TrieNode>());
    }
    node = node->children_.at(key[i]);
  }

  if (node->children_.count(key[klen - 1]) == 0) {  // key not exist
    node->children_[key[klen - 1]] = make_shared<TrieNodeWithValue<T>>(make_shared<T>(std::move(value)));
  } else if (!node->children_[key[klen - 1]]->is_value_node_) {  // not value node
    auto tmp = node->children_.at(key[klen - 1]);
    node->children_[key[klen - 1]] =
        make_shared<TrieNodeWithValue<T>>(tmp->children_, make_shared<T>(std::move(value)));
  } else {  // is value node
    shared_ptr<TrieNode> new_root = root_->Clone();
    Trie new_trie = Trie(new_root);
    shared_ptr<TrieNode> pre = new_root;
    shared_ptr<TrieNode> nxt = root_;
    for (int i = 0; i < klen - 1; i++) {
      nxt = nxt->children_[key[i]];
      pre->children_[key[i]] = nxt->Clone();
      pre = pre->children_[key[i]];
    }
    nxt = nxt->children_[key[klen - 1]];
    pre->children_[key[klen - 1]] = make_shared<TrieNodeWithValue<T>>(nxt->children_, make_shared<T>(std::move(value)));
    return new_trie;
  }

  return *this;

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) -> Trie {
  if (key.empty()) {
    if (!root_->is_value_node_) {
      return *this;
    }
    shared_ptr<TrieNode> new_root = make_shared<TrieNode>(root_->children_);
    return Trie(new_root);
  }

  // check if key exist
  auto node = root_;
  for (char ch : key) {
    if (node->children_.count(ch) == 0) {
      return *this;
    }
    node = node->children_[ch];
  }
  if (!node->is_value_node_) {
    return *this;
  }

  // generate new trie and remove key
  shared_ptr<TrieNode> new_root = root_->Clone();
  shared_ptr<TrieNode> pre = new_root;
  shared_ptr<TrieNode> nxt;
  Trie new_trie = Trie(new_root);

  int klen = key.size();
  for (int i = 0; i < klen; i++) {
    nxt = pre->children_[key[i]];
    if (i == klen - 1) {
      if (nxt->children_.empty()) {
        pre->children_.erase(key[i]);
      } else {
        pre->children_[key[i]] = make_shared<TrieNode>(nxt->children_);
      }

    } else {
      pre->children_[key[i]] = nxt->Clone();
      pre = pre->children_[key[i]];
    }
  }

  return new_trie;
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
