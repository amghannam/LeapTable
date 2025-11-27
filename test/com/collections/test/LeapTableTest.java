package com.collections.test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.util.collections.LeapTable;

final class LeapTableTest {

	/* ========================= basic functionality ========================= */

	@Test
	@DisplayName("Monotone appends and basic get/containsKey work")
	void monotoneAppendAndGet() {
		var lt = new LeapTable<Integer, String>();
		for (int i = 0; i < 100; i++) {
			lt.append(i, "v" + i);
		}

		assertEquals(100, lt.sizeLive());
		assertFalse(lt.isEmpty());

		assertEquals("v0", lt.get(0));
		assertEquals("v50", lt.get(50));
		assertEquals("v99", lt.get(99));

		assertTrue(lt.containsKey(0));
		assertTrue(lt.containsKey(99));
		assertFalse(lt.containsKey(100));

		assertNull(lt.get(100)); // never inserted
	}

	@Test
	@DisplayName("Out-of-order append is rejected")
	void outOfOrderRejected() {
		var lt = new LeapTable<Integer, String>();
		lt.append(10, "a");
		lt.append(10, "b"); // equal is allowed (non-decreasing)
		var ex = assertThrows(IllegalArgumentException.class, () -> lt.append(9, "c"));
		assertTrue(
				ex.getMessage().toLowerCase().contains("non-decreasing")
						|| ex.getMessage().toLowerCase().contains("appended"),
				"Error message should mention the monotonic order constraint");
	}

	@Test
	@DisplayName("First and last key work with and without deletes")
	void firstLastKey() {
		var lt = new LeapTable<Integer, String>();
		assertNull(lt.firstKey());
		assertNull(lt.lastKey());

		for (int i = 0; i < 5; i++) {
			lt.append(i, "v" + i);
		}
		assertEquals(0, lt.firstKey());
		assertEquals(4, lt.lastKey());

		lt.remove(0);
		lt.remove(4);
		assertEquals(1, lt.firstKey());
		assertEquals(3, lt.lastKey());

		lt.compact();
		assertEquals(1, lt.firstKey());
		assertEquals(3, lt.lastKey());
	}

	/* ========================= floor / ceil behavior ========================= */

	@Test
	@DisplayName("floorEntry and ceilEntry around gaps and edges")
	void floorAndCeilWithHoles() {
		var lt = new LeapTable<Integer, String>();
		for (int i = 0; i < 10; i++) {
			lt.append(i * 2, "v" + (i * 2)); // keys: 0,2,4,...,18
		}

		// exact hit
		assertEquals(Map.entry(8, "v8"), lt.floorEntry(8));
		assertEquals(Map.entry(8, "v8"), lt.ceilEntry(8));

		// between 8 and 10
		assertEquals(Map.entry(8, "v8"), lt.floorEntry(9));
		assertEquals(Map.entry(10, "v10"), lt.ceilEntry(9));

		// below smallest
		assertNull(lt.floorEntry(-1));
		assertEquals(Map.entry(0, "v0"), lt.ceilEntry(-1));

		// above largest
		assertEquals(Map.entry(18, "v18"), lt.floorEntry(999));
		assertNull(lt.ceilEntry(999));

		// delete a middle key and see floor/ceil skip it
		lt.remove(8);
		assertNull(lt.get(8));

		var floor = lt.floorEntry(8);
		var ceil = lt.ceilEntry(8);

		assertEquals(Map.entry(6, "v6"), floor);
		assertEquals(Map.entry(10, "v10"), ceil);
	}

	/* ========================= range behavior ========================= */

	@SuppressWarnings("unused")
	@Test
	@DisplayName("Range returns inclusive [from,to] in sorted order")
	void rangeBasic() {
		var lt = new LeapTable<Integer, String>();
		for (int i = 0; i < 20; i++) {
			lt.append(i, "v" + i);
		}

		var keys = new ArrayList<Integer>();
		lt.range(5, 12, (k, v) -> keys.add(k));

		assertEquals(List.of(5, 6, 7, 8, 9, 10, 11, 12), keys);
	}

	@SuppressWarnings("unused")
	@Test
	@DisplayName("Range respects tombstones and empty/invalid intervals")
	void rangeWithDeletesAndEdges() {
		var lt = new LeapTable<Integer, String>();
		for (int i = 0; i < 10; i++) {
			lt.append(i, "v" + i);
		}

		lt.remove(3);
		lt.remove(4);
		lt.remove(7);

		var keys = new ArrayList<Integer>();
		lt.range(0, 9, (k, v) -> keys.add(k));
		assertEquals(List.of(0, 1, 2, 5, 6, 8, 9), keys);

		keys.clear();
		lt.range(20, 30, (k, v) -> keys.add(k));
		assertTrue(keys.isEmpty(), "range outside data should yield nothing");

		keys.clear();
		lt.range(5, 4, (k, v) -> keys.add(k));
		assertTrue(keys.isEmpty(), "range with from > to should yield nothing");
	}

	/* ========================= deletes & compaction ========================= */

	@SuppressWarnings("unused")
	@Test
	@DisplayName("Delete creates tombstones, compact reclaims them")
	void deleteAndCompact() {
		var lt = new LeapTable<Integer, String>();
		for (int i = 0; i < 10; i++) {
			lt.append(i, "v" + i);
		}

		assertEquals("v3", lt.remove(3));
		assertEquals("v4", lt.remove(4));
		assertNull(lt.remove(3)); // second delete is no-op

		assertEquals(8, lt.sizeLive());
		assertEquals(10, lt.sizePhysical());

		// ensure keys are skipped in range
		var keys = new ArrayList<Integer>();
		lt.range(0, 9, (k, v) -> keys.add(k));
		assertEquals(List.of(0, 1, 2, 5, 6, 7, 8, 9), keys);

		lt.compact();

		assertEquals(8, lt.sizeLive());
		assertEquals(8, lt.sizePhysical());

		keys.clear();
		lt.range(0, 9, (k, v) -> keys.add(k));
		assertEquals(List.of(0, 1, 2, 5, 6, 7, 8, 9), keys);
	}

	/*
	 * ========================= null values & string keys =========================
	 */

	@Test
	@DisplayName("Null values are allowed; null keys are rejected")
	void nullKeysAndValues() {
		var lt = new LeapTable<Integer, String>();

		assertThrows(NullPointerException.class, () -> lt.append(null, "x"));

		lt.append(1, null);
		assertNull(lt.get(1)); // cannot distinguish from absent by value alone
		assertTrue(lt.containsKey(1));

		lt.append(2, "hello");
		assertEquals("hello", lt.get(2));
	}

	@Test
	@DisplayName("LeapTable works with String keys and lexicographic order")
	void stringKeys() {
		var lt = new LeapTable<String, Integer>();
		lt.append("A001", 1);
		lt.append("A002", 2);
		lt.append("A010", 10);
		lt.append("B001", 100);

		assertEquals(1, lt.get("A001"));
		assertEquals(10, lt.get("A010"));
		assertEquals(100, lt.get("B001"));

		assertEquals("A001", lt.firstKey());
		assertEquals("B001", lt.lastKey());

		// floor/ceil lexicographically
		assertEquals("A002", lt.floorEntry("A005").getKey());
		assertEquals("A010", lt.ceilEntry("A005").getKey());
	}

	/* ========================= iterator & snapshots ========================= */

	@Test
	@DisplayName("Iterator yields live entries in ascending key order")
	void iteratorOrder() {
		var lt = new LeapTable<Integer, String>();
		for (int i = 0; i < 8; i++) {
			lt.append(i, "v" + i);
		}
		lt.remove(2);
		lt.remove(5);

		var iterKeys = new ArrayList<Integer>();
		for (var e : lt) {
			iterKeys.add(e.getKey());
		}

		assertEquals(List.of(0, 1, 3, 4, 6, 7), iterKeys);
	}

	@Test
	@DisplayName("keys() and valuesList() return immutable snapshots")
	void snapshots() {
		var lt = new LeapTable<Integer, String>();
		for (int i = 0; i < 5; i++) {
			lt.append(i, "v" + i);
		}
		lt.remove(1);
		lt.remove(3);

		var keys = lt.keys();
		var values = lt.valuesList();

		assertEquals(List.of(0, 2, 4), keys);
		assertEquals(List.of("v0", "v2", "v4"), values);

		assertThrows(UnsupportedOperationException.class, () -> keys.add(99));
		assertThrows(UnsupportedOperationException.class, () -> values.add("x"));
	}

	/*
	 * ========================= randomized property-style tests
	 * =========================
	 */

	@Test
	@DisplayName("Randomized comparison against TreeMap for get/floor/ceil/range before and after compact")
	void randomizedAgainstTreeMap() {
		var rnd = new Random(12345);

		var lt = new LeapTable<Integer, Integer>();
		var tm = new TreeMap<Integer, Integer>();

		// 1) Build structure with monotone appends
		int n = 5_000;
		for (int i = 0; i < n; i++) {
			lt.append(i, i * 10);
			tm.put(i, i * 10);
		}

		// 2) Randomly delete some keys
		for (int i = 0; i < n; i++) {
			if (rnd.nextDouble() < 0.25) {
				lt.remove(i);
				tm.remove(i);
			}
		}

		// Helper lambdas to compare behaviors
		@SuppressWarnings("unused")
		var checkPointOps = (Runnable) () -> {
			for (int t = 0; t < 2_000; t++) {
				int k = ThreadLocalRandom.current().nextInt(-100, n + 100);

				// get
				Integer expected = tm.get(k);
				Integer actual = lt.get(k);
				assertEquals(expected, actual, "get mismatch at key " + k);

				// floor
				var ef = tm.floorEntry(k);
				var af = lt.floorEntry(k);
				if (ef == null) {
					assertNull(af, "floorEntry should be null at key " + k);
				} else {
					assertNotNull(af, "floorEntry should be non-null at key " + k);
					assertEquals(ef.getKey(), af.getKey(), "floor key mismatch at " + k);
					assertEquals(ef.getValue(), af.getValue(), "floor value mismatch at " + k);
				}

				// ceil
				var ec = tm.ceilingEntry(k);
				var ac = lt.ceilEntry(k);
				if (ec == null) {
					assertNull(ac, "ceilEntry should be null at key " + k);
				} else {
					assertNotNull(ac, "ceilEntry should be non-null at key " + k);
					assertEquals(ec.getKey(), ac.getKey(), "ceil key mismatch at " + k);
					assertEquals(ec.getValue(), ac.getValue(), "ceil value mismatch at " + k);
				}

				// range
				int a = ThreadLocalRandom.current().nextInt(-50, n + 50);
				int b = ThreadLocalRandom.current().nextInt(-50, n + 50);
				int from = Math.min(a, b);
				int to = Math.max(a, b);

				var expectedKeys = new ArrayList<Integer>(tm.subMap(from, true, to, true).keySet());
				var actualKeys = new ArrayList<Integer>();
				lt.range(from, to, (kk, vv) -> actualKeys.add(kk));
				assertEquals(expectedKeys, actualKeys, "range mismatch for [" + from + "," + to + "]");
			}
		};

		// Check before compact
		checkPointOps.run();

		// 3) Compact and re-check invariants
		lt.compact();

		assertEquals(tm.size(), lt.sizeLive(), "sizeLive mismatch after compact");

		checkPointOps.run();
	}

	/*
	 * ========================= serialization round-trip =========================
	 */

	@Test
	@DisplayName("Serialization round-trip preserves data and tombstones")
	void serializationRoundTrip() throws Exception {
		var lt = new LeapTable<Integer, String>();
		for (int i = 0; i < 50; i++) {
			lt.append(i, "v" + i);
		}
		lt.remove(7);
		lt.remove(8);

		byte[] bytes = toBytes(lt);
		LeapTable<Integer, String> copy = fromBytes(bytes);

		assertNull(copy.get(7));
		assertNull(copy.get(8));
		assertEquals("v6", copy.get(6));
		assertEquals("v9", copy.get(9));
		assertEquals(48, copy.sizeLive());
		assertEquals(50, copy.sizePhysical());
	}

	/* ========================= helpers ========================= */

	private static byte[] toBytes(Serializable obj) throws IOException {
		try (var bos = new ByteArrayOutputStream(); var oos = new ObjectOutputStream(bos)) {
			oos.writeObject(obj);
			oos.flush();
			return bos.toByteArray();
		}
	}

	@SuppressWarnings("unchecked")
	private static <K extends Comparable<? super K>, V> LeapTable<K, V> fromBytes(byte[] bytes)
			throws IOException, ClassNotFoundException {
		try (var bis = new ByteArrayInputStream(bytes); var ois = new ObjectInputStream(bis)) {
			return (LeapTable<K, V>) ois.readObject();
		}
	}
}
