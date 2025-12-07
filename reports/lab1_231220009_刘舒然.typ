// ==========================================
// 1. Template Setup (模版设置 - 不要随意修改)
// ==========================================

#let project(
  title: "",
  student_info: (),
  body
) = {
  // 设置文档元数据
  set document(author: student_info.name, title: title)
  
  // 设置页面尺寸与页边距
  set page(
    paper: "a4",
    margin: (x: 2.5cm, y: 2.5cm),
    numbering: "1 / 1",
  )

  // 设置字体 (中英文混排)
  // 英文使用 Times New Roman，中文优先使用 Source Han Serif SC (思源宋体) 或 SimSun (宋体)
  set text(
    font: ("Times New Roman", "SimSun"),
    size: 12pt,
    lang: "zh"
  )

  // 设置标题样式
  set heading(numbering: "1.1")
  show heading: it => {
    set text(font: ("Arial"), weight: "bold")
    v(0.5em)
    it
    v(0.3em)
  }

  // 设置代码块样式
  show raw.where(block: true): block.with(
    fill: luma(240),
    inset: 10pt,
    radius: 4pt,
    width: 100%,
  )
  // 代码字体
  show raw: set text(font: ("Consolas", "Courier New"), size:12pt)

  // 标题展示
  align(center)[
    #text(20pt, weight: "bold")[#title]
    #v(1em)
  ]

  // === 顶部学生信息表格 (根据实验要求定制) ===
  align(center)[
    #table(
      columns: (1.5fr, 1fr, 3fr, 2fr),
      inset: 8pt,
      align: center + horizon,
      stroke: 1pt + black,
      [*学号*], [*姓名*], [*邮箱*], [*完成题目*],
      [#student_info.id], [#student_info.name], [#student_info.email], [#student_info.tasks]
    )
  ]
  
  v(2em)

  // 正文段落设置：两端对齐，首行缩进
  set par(justify: true, first-line-indent: 2em, leading: 1em)
  // 修复首行缩进对标题的影响
  show heading: it => { set par(first-line-indent: 0em); it }
  
  body
}

// 辅助函数：用于显示测试结果的绿色/红色块
#let test_result(name, passed: true) = {
  let color = if passed { rgb("#e6fffa") } else { rgb("#fff5f5") }
  let stroke_color = if passed { rgb("#319795") } else { rgb("#e53e3e") }
  let text_color = if passed { rgb("#2c7a7b") } else { rgb("#c53030") }
  let icon = if passed { "✅" } else { "❌" }
  
  block(
    fill: color,
    stroke: (left: 4pt + stroke_color),
    inset: 10pt,
    width: 100%,
    radius: 2pt
  )[
    #text(weight: "bold", fill: text_color)[#icon #name 单元测试]
  ]
}

// ==========================================
// 2. Main Document (文档正文 - 在此填空)
// ==========================================

// --- 用户配置区 ---
#show: project.with(
  title: "实验 1: 存储管理实验报告",
  student_info: (
    id: "231220009",                 // 修改为你的学号
    name: "刘舒然",                   // 修改为你的姓名
    email: "231220009@smail.nju.edu.cn", // 修改为你的邮箱
    tasks: "1 / 2 / 3 / f1 / f2"    // 修改你完成的题目
  )
)

// --- 正文开始 ---

#outline(
  title: "目录",
  indent: auto,
  depth: 3
)
#pagebreak()

= 构建和准备

#h(2em)我的实验环境为 `Arch Linux` 系统，内核版本为 `Linux arch 6.17.9-arch1-1 #1 SMP PREEMPT_DYNAMIC Mon, 24 Nov 2025 15:21:09 +0000 x86_64 GNU/Linux`，使用的 GCC 版本为 `gcc (GCC) 15.2.1 20251112`。然而，在首次 `make` 的过程中遇到了问题：
#image("/assets/image.png")
#h(2em)这需要在 `test/storage/replacer_test.cpp` 中添加 `#include <algorithm>` 头文件以解决。

之后，我仔细阅读了源代码，理解了完成实验一主要需要了解的两个数据结构：`DiskManager` 和 `Frame`。其中，`DiskManager` 负责数据库系统与底层文件系统之间的交互，而 `Frame` 则表示代表了缓冲池中的一个物理内存块，是 `BufferPoolManager` 管理的基本单位。

= t1. LRU

== 实现思路

- `Pin`：如果页面已在链表中，将其标记为 `is_evictable = false`，并移动到链表尾部。如果不在链表中，则新建节点加入尾部
- `Unpin`：将页面的状态更新为     `is_evictable = true`。如果页面意外不在链表中，则作为新页面加入 MRU。
- `Victim`：找到第一个 `is_evictable == true` 的页面。将其从链表和哈希表中彻底移除。

== 测试结果
#test_result("ReplacerTest.LRU", passed: true)
#image("/assets/image-1.png")

#line(length: 100%, stroke: 0.5pt + gray)

= t2. Buffer Pool Manager

== 实现思路
// 解释 FetchPage, UnpinPage, FlushPage 等核心函数的逻辑。
// 重点描述 Page ID 和 Frame ID 的映射关系维护，以及 Free List 的管理。

Buffer Pool Manager 是内存管理的核心。我维护了 `page_table_` 来记录磁盘页号 (`page_id`) 到内存帧号 (`frame_id`) 的映射。

- *FetchPage*: 当上层请求一个页面时，首先在 `page_table_` 查找。如果存在，直接 Pin 并返回；如果不存在，则调用 `FindFrame` 获取空闲帧（可能触发 LRU Evict），并从磁盘读取数据。
- *DeletePage*: 在删除页面时，需要注意...

== 关键代码逻辑
// 如果有需要展示的代码片段，可以使用代码块
```cpp
// 示例：获取空闲帧的逻辑片段
if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
} else {
    if (!replacer_->Victim(&frame_id)) {
        return nullptr; // 无可用帧
    }
    // 处理脏页写回逻辑...
}
```