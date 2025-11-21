package com.util.collections;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntUnaryOperator;

/**
 * An ordered, append-optimized map backed by a contiguous array and a
 * deterministic skip-index for logarithmic lookups.
 *
 * <p>
 * <strong>Design overview:</strong> All key-value pairs are stored in a single,
 * sorted, append-only base array. A small number of auxiliary "levels" (sampled
 * index arrays) act as a deterministic skip-index. These levels allow lookups
 * to narrow their search interval with a handful of tiny binary searches before
 * finishing on the base array. This yields near–binary-search performance with
 * better cache locality than traditional skip lists.
 *
 * <h2>Tombstones (logical deletion)</h2> Deletions do not physically remove
 * entries from the base array. Instead, each index in the base array
 * corresponds to a bit in a {@link BitSet}. When an entry is deleted, the
 * corresponding bit is marked as a <em>tombstone</em>.
 *
 * <p>
 * Marking tombstones has two core benefits:
 * <ul>
 * <li><strong>Deletes are O(1):</strong> no shifting or array copying.</li>
 * <li><strong>Indices remain stable:</strong> the skip-index levels remain
 * valid because the positions of surviving entries never change.</li>
 * </ul>
 *
 * <p>
 * All read operations (iteration, range scans, lookups) treat tombstoned
 * entries as absent. They remain in memory only as placeholders until the user
 * chooses to compact the structure.
 *
 * <h2>Compaction</h2> The {@link #compact()} operation physically removes all
 * tombstoned entries and rebuilds the skip-index levels. After compaction:
 *
 * <ul>
 * <li>the base array contains only live entries,</li>
 * <li>the tombstone BitSet is reset, and</li>
 * <li>the skip-index levels are rebuilt for optimal search performance.</li>
 * </ul>
 *
 * <p>
 * Compaction runs in O(n) time and is entirely optional; many workloads can
 * defer or avoid it depending on memory and performance needs.
 *
 * <h2>Usage constraints</h2>
 * <ul>
 * <li>Keys must be appended in <strong>non-decreasing</strong> order.
 * Out-of-order inserts are not supported.</li>
 * <li>Null keys are disallowed. Null values are permitted.</li>
 * <li>Concurrent writes are not supported without external synchronization.
 * Reads are wait-free and safe across level rebuilds.</li>
 * </ul>
 *
 * <h2>Complexity</h2>
 * <ul>
 * <li>{@code putAppend}: amortized O(1)</li>
 * <li>{@code get}, {@code floor}, {@code ceil}: O(log n)</li>
 * <li>{@code range}: O(log n + m), where m is result count</li>
 * <li>{@code compact}: O(n)</li>
 * </ul>
 *
 * <p>
 * This structure is especially useful for workloads with:
 * <ul>
 * <li>monotonic streams (timestamps, sequence numbers),</li>
 * <li>frequent queries and infrequent deletes,</li>
 * <li>heavy range scanning,</li>
 * <li>a need for predictable, cache-friendly performance.</li>
 * </ul>
 *
 * @param <K> key type, must implement {@link Comparable}
 * @param <V> value type
 * @author Ahmed Ghannam
 * @version 1.0
 */
public final class LeapTable<K extends Comparable<? super K>, V> implements Iterable<Map.Entry<K, V>>, Serializable {

	@Serial
	private static final long serialVersionUID = 1L;

	/* ------------------------------ storage ------------------------------ */

	/** Immutable key-value pair used by the base array. */
	private record Entry<K, V>(K key, V value) implements Map.Entry<K, V>, Serializable {
		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V v) {
			throw new UnsupportedOperationException("immutable entry");
		}
	}

	/** Base: sorted by key; append-only semantics. */
	private final List<Entry<K, V>> base = new ArrayList<>();

	/** Tombstones aligned to base indices (true => deleted). */
	private BitSet tombstone = new BitSet();

	/**
	 * Levels (sampled indices into {@code base}). Derived state; rebuilt on demand
	 * and after deserialization.
	 */
	private transient AtomicReference<List<int[]>> levelsRef;

	/** Next geometric threshold for rebuilding the levels. */
	private int nextRebuildAt = 1;

	/* ------------------------------ construction ------------------------------ */

	/** Creates an empty LeapTable. */
	public LeapTable() {
		this.levelsRef = new AtomicReference<>(List.of());
	}

	/**
	 * Creates a LeapTable and bulk-appends from the given sorted map. The map must
	 * be ordered in non-decreasing key order.
	 */
	public LeapTable(Map<K, V> sorted) {
		this.levelsRef = new AtomicReference<>(List.of());
		if (!sorted.isEmpty() && !(sorted instanceof SortedMap<?, ?>)) {
			// Still allow if user promises order: we validate monotonicity as we go.
		}
		var last = (K) null;
		for (var e : sorted.entrySet()) {
			var k = Objects.requireNonNull(e.getKey(), "null keys not permitted");
			if (last != null && k.compareTo(last) < 0) {
				throw new IllegalArgumentException("Input not in non-decreasing key order");
			}
			base.add(new Entry<>(k, e.getValue()));
			last = k;
		}
		if (!base.isEmpty()) {
			tombstone = new BitSet(base.size());
			rebuildLevelsAndThreshold();
		}
	}

	/*
	 * ------------------------------ serialization hooks
	 * ------------------------------
	 */

	@Serial
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
	}

	@Serial
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		// Recreate transient derived state
		levelsRef = new AtomicReference<>(List.of());
		if (tombstone == null) {
			tombstone = new BitSet(base.size());
		}
		if (!base.isEmpty()) {
			rebuildLevelsAndThreshold();
		} else {
			nextRebuildAt = 1;
		}
	}

	/* ------------------------------ core API ------------------------------ */

	/**
	 * Appends {@code (key, value)}. Keys must be in non-decreasing order relative
	 * to the last appended key.
	 *
	 * @throws IllegalArgumentException if {@code key} would break monotone order
	 * @throws NullPointerException     if {@code key} is null
	 */
	public void putAppend(K key, V value) {
		Objects.requireNonNull(key, "key");
		if (!base.isEmpty() && key.compareTo(base.get(base.size() - 1).key()) < 0) {
			throw new IllegalArgumentException("Keys must be appended in non-decreasing order");
		}
		base.add(new Entry<>(key, value));
		ensureTombstoneCapacity();
		maybeRebuildLevels();
	}

	/** Returns the value for {@code key}, or {@code null} if absent or deleted. */
	public V get(K key) {
		int idx = findExactIndex(key);
		if (idx < 0)
			return null;
		return tombstone.get(idx) ? null : base.get(idx).value();
	}

	/**
	 * Removes {@code key} by placing a tombstone; returns previous value or null.
	 */
	public V delete(K key) {
		int idx = findExactIndex(key);
		if (idx < 0 || tombstone.get(idx))
			return null;
		tombstone.set(idx, true);
		return base.get(idx).value();
	}

	/** Returns the number of non-deleted entries (O(n)). */
	public int sizeLive() {
		return base.size() - tombstone.cardinality();
	}

	/** Returns the total physical size including tombstoned entries. */
	public int sizePhysical() {
		return base.size();
	}

	/** Returns {@code true} if there are no non-deleted entries. */
	public boolean isEmpty() {
		return sizeLive() == 0;
	}

	/** Removes all data and resets the LeapTable. */
	public void clear() {
		base.clear();
		tombstone = new BitSet();
		levelsRef.set(List.of());
		nextRebuildAt = 1;
	}

	/** Returns the smallest key, or {@code null} if empty. */
	public K firstKey() {
		int i = nextLiveForward(0);
		return (i >= 0) ? base.get(i).key() : null;
	}

	/** Returns the largest key, or {@code null} if empty. */
	public K lastKey() {
		int i = nextLiveBackward(base.size() - 1);
		return (i >= 0) ? base.get(i).key() : null;
	}

	/** Returns the entry with the greatest key {@code <= key}, or {@code null}. */
	public Map.Entry<K, V> floorEntry(K key) {
		return materialize(findFloorIndex(key));
	}

	/** Returns the entry with the smallest key {@code >= key}, or {@code null}. */
	public Map.Entry<K, V> ceilEntry(K key) {
		return materialize(findCeilIndex(key));
	}

	/**
	 * Streams entries with keys in the closed interval [{@code fromKey},
	 * {@code toKey}]. The {@code consumer} is invoked in ascending key order.
	 */
	public void range(K fromKey, K toKey, BiConsumer<? super K, ? super V> consumer) {
		Objects.requireNonNull(fromKey, "fromKey");
		Objects.requireNonNull(toKey, "toKey");
		if (!lessOrEqual(fromKey, toKey))
			return;
		int i = findCeilIndex(fromKey);
		if (i < 0)
			return;
		for (; i < base.size(); i++) {
			if (tombstone.get(i))
				continue;
			var e = base.get(i);
			if (e.key().compareTo(toKey) > 0)
				break;
			consumer.accept(e.key(), e.value());
		}
	}

	/**
	 * Creates an iterator over non-deleted entries in ascending key order. The
	 * iterator is weakly consistent: it reflects the structure at creation time.
	 */
	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		return new Iterator<>() {
			int i = nextLiveForward(0);

			@Override
			public boolean hasNext() {
				return i >= 0 && i < base.size();
			}

			@Override
			public Map.Entry<K, V> next() {
				if (!hasNext())
					throw new NoSuchElementException();
				var e = base.get(i);
				i = nextLiveForward(i + 1);
				return e;
			}
		};
	}

	/**
	 * Compacts the table by physically removing tombstoned entries and rebuilding
	 * the levels. Complexity O(n).
	 */
	public void compact() {
		if (tombstone.isEmpty() || tombstone.cardinality() == 0) {
			if (!base.isEmpty())
				rebuildLevelsAndThreshold();
			return;
		}
		var fresh = new ArrayList<Entry<K, V>>(sizeLive());
		forEachLiveIndex(i -> fresh.add(base.get(i)));
		base.clear();
		base.addAll(fresh);
		tombstone = new BitSet(base.size());
		rebuildLevelsAndThreshold();
	}

	/* ------------------------------ searching ------------------------------ */

	/** Returns exact index or negative insertion point (~pos). */
	private int findExactIndex(K key) {
		Objects.requireNonNull(key, "key");
		if (base.isEmpty())
			return -1;

		int lo = 0;
		int hi = base.size() - 1;

		var levels = levelsRef.get();
		for (int lev = levels.size() - 1; lev >= 0; lev--) {
			var idxs = levels.get(lev);
			int left = lowerBoundIndex(idxs, lo);
			int right = upperBoundIndex(idxs, hi);
			if (left > right)
				continue;

			int pos = binarySearchLevel(key, idxs, left, right);
			if (pos >= 0)
				return idxs[pos];

			int ip = -pos - 1;
			int newLo = (ip - 1 >= left) ? idxs[ip - 1] : lo;
			int newHi = (ip <= right) ? ((ip == idxs.length) ? hi : idxs[Math.min(ip, right)]) : hi;
			lo = newLo;
			hi = newHi;
		}

		return binarySearchBase(key, lo, hi);
	}

	/** Returns floor index (<= key) or -1 if none. */
	private int findFloorIndex(K key) {
		return findBoundIndex(key, true);
	}

	/** Returns ceil index (>= key) or -1 if none. */
	private int findCeilIndex(K key) {
		return findBoundIndex(key, false);
	}

	/**
	 * Shared bound finder for floor/ceil.
	 *
	 * @param key   search key
	 * @param floor if true, find floor; otherwise find ceil
	 * @return index into {@code base} or -1 if none
	 */
	private int findBoundIndex(K key, boolean floor) {
		int idx = findExactIndex(key);
		if (idx >= 0) {
			if (!tombstone.get(idx)) {
				return idx;
			}
			return floor ? nextLiveBackward(idx - 1) : nextLiveForward(idx + 1);
		}
		int ip = -idx - 1;
		if (floor) {
			return nextLiveBackward(ip - 1);
		}
		int cand = nextLiveForward(ip);
		return (cand < base.size()) ? cand : -1;
	}

	/* ------------------------------ internals ------------------------------ */

	private static <K extends Comparable<? super K>, V> int compareKey(Entry<K, V> e, K key) {
		return e.key().compareTo(key);
	}

	private boolean lessOrEqual(K a, K b) {
		return a.compareTo(b) <= 0;
	}

	private Map.Entry<K, V> materialize(int idx) {
		if (idx < 0 || idx >= base.size() || tombstone.get(idx))
			return null;
		return base.get(idx);
	}

	private void ensureTombstoneCapacity() {
		int need = base.size() - 1;
		if (need >= tombstone.size())
			tombstone.set(need, false);
	}

	/** Rebuilds levels and recomputes the next rebuild threshold. */
	private void rebuildLevelsAndThreshold() {
		rebuildLevels();
		int n = Math.max(1, base.size());
		nextRebuildAt = Math.max(2, Integer.highestOneBit(n) << 1);
	}

	private void maybeRebuildLevels() {
		if (base.size() >= nextRebuildAt) {
			rebuildLevelsAndThreshold();
		}
	}

	/** Builds new sampled levels and swaps them atomically for wait-free reads. */
	private void rebuildLevels() {
		if (base.isEmpty()) {
			levelsRef.set(List.of());
			return;
		}
		int n = base.size();
		var newLevels = new ArrayList<int[]>();
		for (int level = 0;; level++) {
			int stride = 1 << (level + 1);
			if (stride >= n)
				break;
			int sz = (n + stride - 1) / stride;
			var idxs = new int[sz];
			for (int p = 0, i = 0; i < n; i += stride)
				idxs[p++] = i;
			newLevels.add(idxs);
			if (sz <= 1024)
				break;
		}
		levelsRef.set(List.copyOf(newLevels));
	}

	private int binarySearchLevel(K key, int[] idxs, int left, int right) {
		return binarySearch(key, left, right, i -> idxs[i]);
	}

	private int binarySearchBase(K key, int lo, int hi) {
		return binarySearch(key, lo, hi, i -> i);
	}

	/**
	 * Shared binary search used by both base and level searches.
	 * <p>
	 * The search space is in the coordinate system [lo, hi] and the
	 * {@code indexResolver} maps a coordinate into a base-array index.
	 *
	 * @return found index in the coordinate system, or {@code -(insertionPoint+1)}
	 */
	private int binarySearch(K key, int lo, int hi, IntUnaryOperator indexResolver) {
		int l = lo;
		int r = hi;
		while (l <= r) {
			int m = (l + r) >>> 1;
			int baseIdx = indexResolver.applyAsInt(m);
			int c = compareKey(base.get(baseIdx), key);
			if (c == 0)
				return m;
			if (c < 0)
				l = m + 1;
			else
				r = m - 1;
		}
		return -(l + 1);
	}

	private int nextLiveForward(int i) {
		return nextLiveFrom(i, +1);
	}

	private int nextLiveBackward(int i) {
		return nextLiveFrom(i, -1);
	}

	/**
	 * Scans for the next non-tombstoned index from {@code start} in {@code step}
	 * (+1 / -1).
	 */
	private int nextLiveFrom(int start, int step) {
		if (base.isEmpty())
			return -1;
		int j = (step > 0) ? Math.max(0, start) : Math.min(start, base.size() - 1);
		while (j >= 0 && j < base.size()) {
			if (!tombstone.get(j))
				return j;
			j += step;
		}
		return -1;
	}

	private int lowerBoundIndex(int[] idxs, int bound) {
		return boundIndex(idxs, bound, true);
	}

	private int upperBoundIndex(int[] idxs, int bound) {
		return boundIndex(idxs, bound, false);
	}

	/**
	 * Shared bound helper:
	 * <ul>
	 * <li>lower: first position with idxs[pos] >= bound; returns idxs.length if
	 * none</li>
	 * <li>upper: last position with idxs[pos] <= bound; returns -1 if none</li>
	 * </ul>
	 */
	private int boundIndex(int[] idxs, int bound, boolean lower) {
		int lo = 0;
		int hi = idxs.length - 1;
		int ans = lower ? idxs.length : -1;

		while (lo <= hi) {
			int mid = (lo + hi) >>> 1;
			if (idxs[mid] >= bound) {
				if (lower) {
					ans = mid;
					hi = mid - 1;
				} else {
					hi = mid - 1;
				}
			} else { // idxs[mid] < bound
				if (!lower) {
					ans = mid;
				}
				lo = mid + 1;
			}
		}
		return ans;
	}

	/**
	 * Runs {@code action} for each index {@code i} where the entry at {@code i} is
	 * live (not tombstoned), in ascending index order.
	 */
	private void forEachLiveIndex(IntConsumer action) {
		for (int i = 0; i < base.size(); i++) {
			if (!tombstone.get(i)) {
				action.accept(i);
			}
		}
	}

	/* ------------------------------ convenience ------------------------------ */

	/** Returns {@code true} if the key exists and is not tombstoned. */
	public boolean containsKey(K key) {
		int idx = findExactIndex(key);
		return idx >= 0 && !tombstone.get(idx);
	}

	/** Applies {@code action} to each live entry in ascending key order. */
	public void forEach(Consumer<? super Map.Entry<K, V>> action) {
		Objects.requireNonNull(action, "action");
		for (var e : this)
			action.accept(e);
	}

	/** Returns an unmodifiable {@code List<K>} snapshot of live keys. */
	public List<K> keys() {
		var out = new ArrayList<K>(sizeLive());
		forEachLiveIndex(i -> out.add(base.get(i).key()));
		return List.copyOf(out);
	}

	/** Returns an unmodifiable {@code List<V>} snapshot of live values. */
	public List<V> valuesList() {
		var out = new ArrayList<V>(sizeLive());
		forEachLiveIndex(i -> out.add(base.get(i).value()));
		return List.copyOf(out);
	}

	/** Returns a string with live entries (for diagnostics). */
	@Override
	public String toString() {
		var sb = new StringBuilder("LeapTable{");
		boolean first = true;
		for (var e : this) {
			if (!first)
				sb.append(", ");
			sb.append(e.getKey()).append('=').append(e.getValue());
			first = false;
		}
		return sb.append('}').toString();
	}
}
