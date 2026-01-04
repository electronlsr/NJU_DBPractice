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
// Created by ziqi on 2024/7/18.
//

#include "executor_idxscan.h"
#include "common/value.h"
#include "expr/condition_expr.h"
#include <algorithm>

namespace njudb {

IdxScanExecutor::IdxScanExecutor(TableHandle *tbl, IndexHandle *idx, ConditionVec conds, bool is_ascending)
    : AbstractExecutor(Basic), tbl_(tbl), idx_(idx), conds_(std::move(conds)), is_ascending_(is_ascending),
      needs_first_record_check_(false), needs_last_record_check_(false), start_idx_(0), end_idx_(0), current_idx_(0)
{
}

void IdxScanExecutor::GenerateRangeKeys()
{
  auto                   &schema = idx_->GetKeySchema();
  std::vector<ValueSptr> low_vals;
  std::vector<ValueSptr> high_vals;

  for (size_t i = 0; i < schema.GetFieldCount(); ++i) {
    auto type = schema.GetFieldAt(i).field_.field_type_;
    low_vals.push_back(ValueFactory::CreateMinValueForType(type));
    high_vals.push_back(ValueFactory::CreateMaxValueForType(type));
  }

  for (size_t i = 0; i < schema.GetFieldCount(); ++i) {
    auto &field = schema.GetFieldAt(i);
    bool  found = false;
    for (auto &cond : conds_) {
      if (cond.GetLCol().field_.field_name_ == field.field_.field_name_) {
        if (cond.GetRhsType() == kValue) {
          auto val = cond.GetRVal();
          if (cond.GetOp() == OP_EQ) {
            low_vals[i]  = val;
            high_vals[i] = val;
            found        = true;
          } else if (cond.GetOp() == OP_GT || cond.GetOp() == OP_GE) {
            low_vals[i] = val;
            if (cond.GetOp() == OP_GT) needs_first_record_check_ = true;
            found = true;
            goto done;
          } else if (cond.GetOp() == OP_LT || cond.GetOp() == OP_LE) {
            high_vals[i] = val;
            if (cond.GetOp() == OP_LT) needs_last_record_check_ = true;
            found = true;
            goto done;
          }
        }
      }
    }
    if (!found) break;
  }
done:
  low_  = std::make_unique<Record>(&schema, low_vals, INVALID_RID);
  high_ = std::make_unique<Record>(&schema, high_vals, INVALID_RID);
}

void IdxScanExecutor::Init()
{
  GenerateRangeKeys();
  rids_ = idx_->SearchRange(*low_, *high_);

  start_idx_ = 0;
  end_idx_   = rids_.size();

  if (needs_first_record_check_ && !rids_.empty()) {
    Record key(&idx_->GetKeySchema(), *tbl_->GetRecord(rids_[0]));
    if (Record::Compare(key, *low_) == 0) {
      start_idx_ = 1;
    }
  }

  if (needs_last_record_check_ && !rids_.empty() && end_idx_ > start_idx_) {
    Record key(&idx_->GetKeySchema(), *tbl_->GetRecord(rids_[end_idx_ - 1]));
    if (Record::Compare(key, *high_) == 0) {
      end_idx_--;
    }
  }

  if (!is_ascending_) {
    std::reverse(rids_.begin() + start_idx_, rids_.begin() + end_idx_);
  }

  current_idx_ = start_idx_;
  if (!IsEnd()) {
    record_ = tbl_->GetRecord(rids_[current_idx_]);
  }
}

void IdxScanExecutor::Next()
{
  current_idx_++;
  if (!IsEnd()) {
    record_ = tbl_->GetRecord(rids_[current_idx_]);
  }
}

auto IdxScanExecutor::IsEnd() const -> bool { return current_idx_ >= end_idx_; }

auto IdxScanExecutor::GetOutSchema() const -> const RecordSchema * { return &tbl_->GetSchema(); }

}  // namespace njudb