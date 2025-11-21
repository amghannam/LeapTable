# LeapTable

*A deterministic, cache-friendly, append-optimized ordered map for Java.*

LeapTable is a high-performance data structure designed for workloads where keys arrive in **non-decreasing order** (timestamps, sequence numbers, logs, metrics, event streams). It combines a contiguous sorted array with a deterministic skip-index to provide extremely predictable performance for point lookups, range scans, and append-only ingestion.

---

## Features

- 🚀 **O(1) amortized append** (monotone keys)  
- 🔍 **O(log n) lookups** (`get`, `floor`, `ceil`)  
- 📈 **O(log n + m) range scans** (single lookup + linear walk)  
- 🪦 **Tombstone deletes**: O(1), index-stable logical deletion  
- 🧹 **Optional compaction** to reclaim space  
- 🧠 **Deterministic skip-index** (no randomness like skip lists)  
- 🔄 **Serializable**, skip-index rebuilt after deserialization  
- 💡 Perfect for **time-series**, logs, event ingestion, and snapshots  

---

## Why LeapTable?

LeapTable excels in scenarios where:

- Keys always increase (e.g., timestamps, auto-incrementing IDs)
- You perform many reads and infrequent writes/deletes
- Range scans matter (metrics, analytics, log views)
- Memory locality is important (array-based, not node-based)
- You want predictable performance (no randomization)

It is essentially a **cache-friendly skip-list alternative** for append-only ordered data.

---

## How It Works

LeapTable is designed for **fast read access** and efficient handling of deletes by utilizing an immutable base array, logical deletion markers, and a layered indexing structure.

---

### 1. Contiguous Base Array

All entries are stored in a single, sorted, append-only array.

> `base`: $[e_0, e_1, e_2, e_3, e_4, \dots ]$

* **Benefit:** This structure is great for **sequential scans** and significantly improves performance due to **CPU cache locality**.

---

### 2. Tombstones (Logical Deletes)

Instead of physically removing entries and incurring expensive memory shifting, LeapTable uses a separate structure to mark entries for logical deletion:

| Base Array (Key, Value) | Tombstone Array (Flag) |
| :---: | :---: |
| $(1, A)$ | $0$ |
| $(2, B)$ | $1$ |
| $(3, C)$ | $0$ |
| $(4, D)$ | $0$ |

* **Mechanism:** A `1` in the tombstone array marks an entry as deleted.
* **Read Access:** **Tombstoned entries are skipped** in all read and scan operations.
* **Physical Removal:** Physical removal of deleted entries happens only during the **Compaction** phase (see section 4).

---

### 3. Skip-Index Levels

LeapTable achieves O(log n) lookups using deterministic, stride-based skip-index levels:

* **Level 0:** Samples every $2^{\text{nd}}$ entry (stride 2) $\rightarrow \left[0, 2, 4, 6, \dots \right]$
* **Level 1:** Samples every $4^{\text{th}}$ entry (stride 4) $\rightarrow \left[0, 4, 8, \dots \right]$
* **Level 2:** Samples every $8^{\text{th}}$ entry (stride 8) $\rightarrow \left[0, 8, 16, \dots \right]$
* **Level $k$:** Samples every $2^{k+1}$-th entry


Lookups start at the highest level and walk downward, narrowing the search window before performing a final binary search on the base array.

---

### 4. Compaction (Maintenance)

Compaction is the routine maintenance process for the storage engine.

* **Action 1 (Garbage Collection):** Physically removes all tombstoned (logically deleted) entries from the base array.
* **Action 2 (Rewrite):** The base array is rewritten into a new, fully compacted state.
* **Action 3 (Rebuild Index):** All skip-index levels are rebuilt over the new, compacted base array.

---

## Installation

Add `LeapTable.java` to your project or package it as part of your module.

(Maven/Gradle snippets can be generated on request.)

---

## Usage Example

```java
var lt = new LeapTable<Integer, String>();

lt.putAppend(1, "A");
lt.putAppend(2, "B");
lt.putAppend(3, "C");

System.out.println(lt.get(2));  // "B"

lt.delete(2); // tombstone
System.out.println(lt.get(2));  // null

lt.range(1, 10, (k, v) -> {
    System.out.println(k + " -> " + v);
});

lt.compact(); // physically removes dead entries
```

---

## Performance Characteristics

| Operation  | Complexity     | Notes                           |
| ---------- | -------------- | ------------------------------- |
| putAppend  | O(1) amortized | Monotone keys required          |
| get        | O(log n)       | Deterministic skip-index search |
| floor/ceil | O(log n)       | Uses same skip-index            |
| range      | O(log n + m)   | m = number of results           |
| delete     | O(1)           | Tombstone mark                  |
| compact    | O(n)           | Rebuilds base + skip-index      |

---

## When Not to Use LeapTable

- Keys arrive out of order.
- You require arbitrary insertions into the middle.
- You frequently modify existing values.
- You delete extremely often without compacting

For these workloads, a TreeMap or skip-list may be a better fit.

--- 

## Serialization

LeapTable supports Java serialization.
Skip levels are transient and rebuilt automatically after deserialization.
