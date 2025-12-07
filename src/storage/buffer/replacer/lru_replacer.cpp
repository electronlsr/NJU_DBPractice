/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/17.
//

#include "lru_replacer.h"
#include "common/config.h"
#include "../common/error.h"
namespace njudb {

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);
  
  for (auto it = lru_list_.begin(); it != lru_list_.end(); ++it) {
    if (it->second) {
      *frame_id = it->first;
      lru_hash_.erase(*frame_id);
      lru_list_.erase(it);
      cur_size_--;
      return true;
    }
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  auto it = lru_hash_.find(frame_id);
  
  if (it != lru_hash_.end()) {
    auto list_it = it->second;
    if (list_it->second) {
      list_it->second = false;
      cur_size_--;
    }
    lru_list_.splice(lru_list_.end(), lru_list_, list_it);
  } else {
    lru_list_.push_back({frame_id, false});
    lru_hash_[frame_id] = std::prev(lru_list_.end());
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  auto it = lru_hash_.find(frame_id);
  
  if (it != lru_hash_.end()) {
    auto list_it = it->second;
    if (!list_it->second) {
      list_it->second = true;
      cur_size_++;
    }
  } else {
    if (lru_list_.size() >= max_size_) {
        return;
    }
    lru_list_.push_back({frame_id, true});
    lru_hash_[frame_id] = std::prev(lru_list_.end());
    cur_size_++;
  }
}

auto LRUReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return cur_size_;
}

}  // namespace njudb
