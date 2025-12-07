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

#include "lru_k_replacer.h"
#include "common/config.h"
#include "../common/error.h"
#include <limits>

namespace njudb {

LRUKReplacer::LRUKReplacer(size_t k) : max_size_(BUFFER_POOL_SIZE), k_(k) {}

auto LRUKReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);
  
  if (cur_size_ == 0) {
    return false;
  }
  
  frame_id_t victim_fid = INVALID_FRAME_ID;
  unsigned long long max_dist = 0;
  timestamp_t earliest_ts = std::numeric_limits<timestamp_t>::max();
  
  for (auto &pair : node_store_) {
    auto &node = pair.second;
    if (!node.IsEvictable()) {
      continue;
    }
    
    unsigned long long dist = node.GetBackwardKDistance(cur_ts_);
    
    if (dist == std::numeric_limits<unsigned long long>::max()) {
      timestamp_t node_earliest = node.GetEarliestTimestamp();
      if (max_dist != std::numeric_limits<unsigned long long>::max()) {
         max_dist = std::numeric_limits<unsigned long long>::max();
         victim_fid = pair.first;
         earliest_ts = node_earliest;
      } else {
         if (node_earliest < earliest_ts) {
           victim_fid = pair.first;
           earliest_ts = node_earliest;
         }
      }
    } else {
       if (max_dist != std::numeric_limits<unsigned long long>::max()) {
         if (dist > max_dist) {
           max_dist = dist;
           victim_fid = pair.first;
         }
       }
    }
  }
  
  if (victim_fid != INVALID_FRAME_ID) {
    *frame_id = victim_fid;
    node_store_.erase(victim_fid);
    cur_size_--;
    return true;
  }
  
  return false;
}

void LRUKReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode(frame_id, k_));
  }
  
  auto &node = node_store_[frame_id];
  node.AddHistory(cur_ts_++);
  
  if (node.IsEvictable()) {
    node.SetEvictable(false);
    cur_size_--;
  }
}

void LRUKReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  
  if (node_store_.find(frame_id) == node_store_.end()) {
     if (node_store_.size() >= max_size_) {
       return;
     }
     node_store_.emplace(frame_id, LRUKNode(frame_id, k_));
     cur_size_++; 
     node_store_[frame_id].SetEvictable(true);
     return;
  }
  
  auto &node = node_store_[frame_id];
  if (!node.IsEvictable()) {
    node.SetEvictable(true);
    cur_size_++;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return cur_size_;
}

}  // namespace njudb
