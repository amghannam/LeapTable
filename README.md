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

### 1. Contiguous base array

All entries live in a sorted `ArrayList`:

base: [ e0, e1, e2, e3, e4 ... ]

shell
Copy code

Great for sequential scans and CPU cache locality.

### 2. Tombstones (logical deletes)

Instead of removing entries and shifting memory:

base: [ (1,A), (2,B), (3,C), (4,D) ]
tombstone: 0 1 0 0

sql
Copy code

Tombstoned entries are skipped in all reads.  
Physical removal happens only during compaction.

### 3. Skip-index levels

Deterministic, stride-based sampling:

Level 0: stride 2 → [0, 2, 4, 6, ...]
Level 1: stride 4 → [0, 4, 8, ...]
Level 2: stride 8 → [0, 8, 16, ...]

pgsql
Copy code

Lookups walk levels top-down, narrowing the search window efficiently.

### 4. Compaction

Physically removes tombstoned entries, rewrites the base array, and rebuilds skip-index levels.

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
