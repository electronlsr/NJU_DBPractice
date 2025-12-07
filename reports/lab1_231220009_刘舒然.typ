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

- `Pin`: 如果页面已在链表中，将其标记为 `is_evictable = false`，并移动到链表尾部。如果不在链表中，则新建节点加入尾部
- `Unpin`: 将页面的状态更新为     `is_evictable = true`。如果页面意外不在链表中，则作为新页面加入 MRU。
- `Victim`: 找到第一个 `is_evictable == true` 的页面。将其从链表和哈希表中彻底移除。

== 测试结果
#test_result("ReplacerTest.LRU", passed: true)
#image("/assets/image-1.png")

#line(length: 100%, stroke: 0.5pt + gray)

= t2. Buffer Pool Manager

== 实现思路

- FetchPage:
  - 命中: 如果页面已在 Buffer Pool 中，直接返回对应的 Frame。必须增加引用计数，并调用 `replacer_->Pin()` 通知替换算法该页面正在被使用
  - 未命中:
    1. 调用 `GetAvailableFrame()` 获取一个可用帧
    2. 调用 `UpdateFrame()` 将目标磁盘页加载到该帧中
    3. 返回该页

- GetAvailableFrame: 优先获取空闲帧。若无空闲帧，调用 `replacer_->Victim()` 选择牺牲帧。如果牺牲帧是脏的，必须先调用 `disk_manager_->WritePage()` 将其写回磁盘。最后还要完成清理工作

- UpdateFrame:
  1. *先*调用 frame->Reset() 清除旧状态
  2. 设置新的 fid 和 pid。
  3. 从磁盘读取数据到内存。
  4. 更新映射，Pin 该帧。

- UnpinPage:
  - 减少 Frame 的引用计数。
  - 当参数 is_dirty 为 true 时，将 Frame 标记为脏
  - 当引用计数降为 0 时，调用 `replacer_->Unpin()`，通知替换算法该帧现在可以被淘汰

- FlushPage/DeletePage:
    - FlushPage: 强制将脏页写回磁盘，并清除脏标记
    - DeletePage: 如果页面当前被 Pin 住，则不能删除。删除时需确保脏数据已写回，然后将帧放回 `free_list_`，并从 `page_frame_lookup_` 移除

== 测试结果
#test_result("BufferPoolManagerTest.SimpleTest", passed: true)
#test_result("BufferPoolManagerTest.MultiThread", passed: true)
#image("/assets/image-2.png")