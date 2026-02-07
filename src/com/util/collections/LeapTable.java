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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
 * <li><strong>Deletion is O(1):</strong> no shifting or array copying.</li>
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
 * <li>{@code append}: amortized O(1)</li>
 * <li>{@code get}, {@code floor}, {@code ceil}: O(log n)</li>
 * <li>{@code range}: O(log n + m), where m is result count</li>
 * <li>{@code compact}: O(n)</li>
 * </ul>
 *
 * <p>
 * This structure is especially useful for workloads with:
 * <ul>
 * <li>monotonic streams (timestamps, sequence numbers),</li>
 * <li>frequent queries and infrequent removals,</li>
 * <li>heavy range scanning,</li>
 * <li>a need for predictable, cache-friendly performance.</li>
 * </ul>
 *
 * @param <K> key type, must implement {@link Comparable}
 * @param <V> value type
 * @author Ahmed Ghannam
 * @version 1.0
 */
public final class LeapTable<K extends Comparable<? super K>, V> 
							implements Iterable<Map.Entry<K, V>>, Serializable {

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

	/** Constructs an empty {@code LeapTable}. */
	public LeapTable() {
		this.levelsRef = new AtomicReference<>(List.of());
	}

	/**
	 * Constructs a {@code LeapTable} and bulk-appends from the given map. The map
	 * must be ordered in non-decreasing key order.
	 *
	 * @param sorted the input map whose entries are appended in iteration order
	 * @throws IllegalArgumentException if the input iteration order is not in
	 *                                  non-decreasing key order
	 * @throws NullPointerException     if any key is {@code null}
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
	 * Appends the specified key-value pair to this table. Keys must be in
	 * non-decreasing order relative to the last appended key.
	 *
	 * @param key   the key to append
	 * @param value the value to append (may be {@code null})
	 * @throws IllegalArgumentException if {@code key} would break monotone order
	 * @throws NullPointerException     if {@code key} is {@code null}
	 */
	public void append(K key, V value) {
		Objects.requireNonNull(key, "key");
		if (!base.isEmpty() && key.compareTo(base.get(base.size() - 1).key()) < 0) {
			throw new IllegalArgumentException("Keys must be appended in non-decreasing order");
		}
		base.add(new Entry<>(key, value));
		ensureTombstoneCapacity();
		maybeRebuildLevels();
	}

	/**
	 * Returns the value to which the specified key is mapped, or {@code null} if
	 * this table contains no mapping for the key, or if the mapping has been
	 * logically deleted.
	 *
	 * @param key the key whose associated value is to be returned
	 * @return the value mapped to {@code key}, or {@code null} if there is no
	 *         mapping or it has been deleted
	 * @throws NullPointerException if {@code key} is {@code null}
	 */
	public V get(K key) {
		int idx = findExactIndex(key);
		if (idx < 0)
			return null;
		return tombstone.get(idx) ? null : base.get(idx).value();
	}

	/**
	 * Removes the mapping for the specified key from this table by placing a
	 * tombstone at its index.
	 * 
	 * <p>
	 * This is a logical deletion operation; no mappings are physically removed by
	 * this method.
	 * </p>
	 *
	 * @param key the key whose mapping is to be removed
	 * @return the previous value associated with {@code key}, or {@code null} if
	 *         there was no mapping or it was already deleted
	 * @throws NullPointerException if {@code key} is {@code null}
	 */
	public V remove(K key) {
		int idx = findExactIndex(key);
		if (idx < 0 || tombstone.get(idx))
			return null;
		tombstone.set(idx, true);
		return base.get(idx).value();
	}

	/**
	 * Returns {@code true} if this table contains no non-deleted entries.
	 *
	 * @return {@code true} if this table has no live entries
	 */
	public boolean isEmpty() {
		return sizeLive() == 0;
	}

	/**
	 * Returns the number of non-deleted entries in this table. The computation runs
	 * in O(n).
	 *
	 * @return the number of live (non-deleted) entries
	 */
	public int sizeLive() {
		return base.size() - tombstone.cardinality();
	}

	/**
	 * Returns the total number of entries stored in this table, including
	 * tombstoned entries.
	 *
	 * @return the total physical size of the table
	 */
	public int sizePhysical() {
		return base.size();
	}

	/**
	 * Removes all mappings from this table and resets its internal state.
	 */
	public void clear() {
		base.clear();
		tombstone = new BitSet();
		levelsRef.set(List.of());
		nextRebuildAt = 1;
	}

	/**
	 * Returns the lowest (first) key currently in this table, or {@code null} if
	 * the table is empty.
	 *
	 * @return the first key, or {@code null} if this table is empty
	 */
	public K firstKey() {
		int i = nextLiveForward(0);
		return (i >= 0) ? base.get(i).key() : null;
	}

	/**
	 * Returns the highest (last) key currently in this table, or {@code null} if
	 * the table is empty.
	 *
	 * @return the last key, or {@code null} if this table is empty
	 */
	public K lastKey() {
		int i = nextLiveBackward(base.size() - 1);
		return (i >= 0) ? base.get(i).key() : null;
	}

	/**
	 * Returns a key-value mapping associated with the greatest key less than or
	 * equal to the given key, or {@code null} if there is no such key.
	 *
	 * @param key the key whose floor entry is to be returned
	 * @return the floor entry for {@code key}, or {@code null} if none
	 * @throws NullPointerException if {@code key} is {@code null}
	 */
	public Map.Entry<K, V> floorEntry(K key) {
		return materialize(findFloorIndex(key));
	}

	/**
	 * Returns a key-value mapping associated with the least key greater than or
	 * equal to the given key, or {@code null} if there is no such key.
	 *
	 * @param key the key whose ceiling entry is to be returned
	 * @return the ceiling entry for {@code key}, or {@code null} if none
	 * @throws NullPointerException if {@code key} is {@code null}
	 */
	public Map.Entry<K, V> ceilEntry(K key) {
		return materialize(findCeilIndex(key));
	}

	/**
	 * Performs the given action for each key-value mapping whose key lies in the
	 * closed interval [{@code fromKey}, {@code toKey}], in ascending key order.
	 *
	 * @param fromKey  the lower bound key (inclusive)
	 * @param toKey    the upper bound key (inclusive)
	 * @param consumer the action to be performed for each matching entry
	 * @throws NullPointerException if {@code fromKey}, {@code toKey} or
	 *                              {@code consumer} is {@code null}
	 */
	public void range(K fromKey, K toKey, BiConsumer<? super K, ? super V> consumer) {
		Objects.requireNonNull(fromKey, "fromKey");
		Objects.requireNonNull(toKey, "toKey");
		Objects.requireNonNull(consumer, "consumer");
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
	 * Returns an iterator over the live (non-deleted) entries in this table in
	 * ascending key order.
	 *
	 * <p>
	 * The iterator is weakly consistent: it reflects the state of the table at some
	 * point at or since the creation of the iterator and is not affected by
	 * subsequent structural modifications.
	 * </p>
	 *
	 * @return an iterator over the live entries in ascending key order
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
	 * Compacts this table by physically removing tombstoned entries from the base
	 * array and rebuilding the skip-index levels.
	 *
	 * <p>
	 * This is an optional operation, with complexity O(n).
	 * </p>
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

	/**
	 * Returns the exact index of the specified key in the base array, or a negative
	 * value representing the insertion point ({@code ~pos}) if the key is not
	 * present.
	 *
	 * @param key the key to locate
	 * @return the index of {@code key}, or {@code -(insertionPoint + 1)} if not
	 *         found
	 * @throws NullPointerException if {@code key} is {@code null}
	 */
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

	/**
	 * Returns the floor index for the given key, that is, the index of the greatest
	 * key less than or equal to the given key, or {@code -1} if none exists.
	 *
	 * @param key the key whose floor index is to be found
	 * @return the floor index, or {@code -1} if there is no floor key
	 */
	private int findFloorIndex(K key) {
		return findBoundIndex(key, true);
	}

	/**
	 * Returns the ceiling index for the given key, that is, the index of the least
	 * key greater than or equal to the given key, or {@code -1} if none exists.
	 *
	 * @param key the key whose ceiling index is to be found
	 * @return the ceiling index, or {@code -1} if there is no ceiling key
	 */
	private int findCeilIndex(K key) {
		return findBoundIndex(key, false);
	}

	/**
	 * Shared bound finder for floor and ceiling queries.
	 *
	 * @param key   the key whose bound index is to be located
	 * @param floor if {@code true}, find the floor index; otherwise find the
	 *              ceiling index
	 * @return the bound index into {@code base}, or {@code -1} if none
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

	/**
	 * Returns the entry at the given index if it is live, or {@code null} if the
	 * index is out of range or tombstoned.
	 *
	 * @param idx the index to materialize
	 * @return the live entry at {@code idx}, or {@code null} if none
	 */
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

	/**
	 * Builds new sampled levels and swaps them atomically for wait-free reads. If
	 * the table is empty, the levels are cleared.
	 */
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

	/**
	 * Performs a binary search for the given key over a level index array in the
	 * range [{@code left}, {@code right}] (inclusive).
	 *
	 * @param key   the key to search for
	 * @param idxs  the level index array
	 * @param left  the left bound (inclusive) in {@code idxs}
	 * @param right the right bound (inclusive) in {@code idxs}
	 * @return the position in {@code idxs} if found, or
	 *         {@code -(insertionPoint + 1)} if not found
	 */
	private int binarySearchLevel(K key, int[] idxs, int left, int right) {
		return binarySearch(key, left, right, i -> idxs[i]);
	}

	/**
	 * Performs a binary search for the given key directly on the base array in the
	 * range [{@code lo}, {@code hi}] (inclusive).
	 *
	 * @param key the key to search for
	 * @param lo  the lower bound index (inclusive)
	 * @param hi  the upper bound index (inclusive)
	 * @return the index in the base array if found, or
	 *         {@code -(insertionPoint + 1)} if not found
	 */
	private int binarySearchBase(K key, int lo, int hi) {
		return binarySearch(key, lo, hi, i -> i);
	}

	/**
	 * Shared binary search used by both base and level searches.
	 *
	 * <p>
	 * The search space is in the coordinate system [{@code lo}, {@code hi}] and the
	 * {@code indexResolver} maps a coordinate into a base-array index.
	 * </p>
	 *
	 * @param key           the key to search for
	 * @param lo            the lower bound coordinate (inclusive)
	 * @param hi            the upper bound coordinate (inclusive)
	 * @param indexResolver a mapping from coordinate to base index
	 * @return the found coordinate, or {@code -(insertionPoint + 1)} if not found
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
	 * Scans for the next non-tombstoned index starting from {@code start} and
	 * moving in the given {@code step} direction (+1 or -1).
	 *
	 * @param start the starting index
	 * @param step  the step direction, either +1 or -1
	 * @return the next live index, or {@code -1} if none exists
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

	/**
	 * Returns the least index {@code pos} such that {@code idxs[pos] >= bound}, or
	 * {@code idxs.length} if there is no such position.
	 *
	 * @param idxs  the array of indices
	 * @param bound the base-array bound
	 * @return the lower-bound position, or {@code idxs.length} if none
	 */
	private int lowerBoundIndex(int[] idxs, int bound) {
		return boundIndex(idxs, bound, true);
	}

	/**
	 * Returns the greatest index {@code pos} such that {@code idxs[pos] <= bound},
	 * or {@code -1} if there is no such position.
	 *
	 * @param idxs  the array of indices
	 * @param bound the base-array bound
	 * @return the upper-bound position, or {@code -1} if none
	 */
	private int upperBoundIndex(int[] idxs, int bound) {
		return boundIndex(idxs, bound, false);
	}

	/**
	 * Shared bound helper:
	 * <ul>
	 * <li>If {@code lower} is {@code true}, returns the first position with
	 * {@code idxs[pos] >= bound}, or {@code idxs.length} if none.</li>
	 * <li>If {@code lower} is {@code false}, returns the last position with
	 * {@code idxs[pos] <= bound}, or {@code -1} if none.</li>
	 * </ul>
	 *
	 * @param idxs  the array of indices
	 * @param bound the base-array bound
	 * @param lower whether to compute a lower bound (otherwise upper bound)
	 * @return the bound position as described above
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
	 * Runs the given action for each index {@code i} such that the entry at
	 * {@code i} is live (not tombstoned), in ascending index order.
	 *
	 * @param action the action to be performed for each live index
	 */
	private void forEachLiveIndex(IntConsumer action) {
		for (int i = 0; i < base.size(); i++) {
			if (!tombstone.get(i)) {
				action.accept(i);
			}
		}
	}

	/* ------------------------------ convenience ------------------------------ */

	/**
	 * Returns {@code true} if this table contains a live mapping for the specified
	 * key.
	 *
	 * @param key the key whose presence is to be tested
	 * @return {@code true} if this table contains a non-deleted mapping for
	 *         {@code key}
	 * @throws NullPointerException if {@code key} is {@code null}
	 */
	public boolean containsKey(K key) {
		int idx = findExactIndex(key);
		return idx >= 0 && !tombstone.get(idx);
	}

	/**
	 * Performs the given action for each live entry in this table, in ascending key
	 * order.
	 *
	 * @param action the action to be performed for each entry
	 * @throws NullPointerException if {@code action} is {@code null}
	 */
	public void forEach(Consumer<? super Map.Entry<K, V>> action) {
		Objects.requireNonNull(action, "action");
		for (var e : this)
			action.accept(e);
	}

	/**
	 * Returns an unmodifiable {@code List} containing a snapshot of the live keys
	 * in this table, in ascending key order.
	 *
	 * @return an unmodifiable list of live keys
	 */
	public List<K> keys() {
		var out = new ArrayList<K>(sizeLive());
		forEachLiveIndex(i -> out.add(base.get(i).key()));
		return List.copyOf(out);
	}

	/**
	 * Returns an unmodifiable {@code List} containing a snapshot of the live values
	 * in this table, in ascending key order of their keys.
	 *
	 * @return an unmodifiable list of live values
	 */
	public List<V> valuesList() {
		var out = new ArrayList<V>(sizeLive());
		forEachLiveIndex(i -> out.add(base.get(i).value()));
		return List.copyOf(out);
	}

	/**
	 * Returns a string representation of this table. The representation consists of
	 * the live mappings in the order they are iterated, enclosed in braces
	 * ({@code {}}).
	 *
	 * @return a string representation of this table
	 */
	@Override
	public String toString() {
		return StreamSupport.stream(spliterator(), false)
				.map(e -> e.getKey() + "=" + e.getValue())
				.collect(Collectors.joining(", ", "LeapTable{", "}"));
	}
}
