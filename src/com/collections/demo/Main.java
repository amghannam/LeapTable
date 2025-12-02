package com.collections.demo;

import com.util.collections.LeapTable;

/**
 * Demo class for LeapTable
 */
public final class Main {

	public static void main(String[] args) {

		System.out.println("=== LeapTable Demo ===");

		// Create a LeapTable mapping integers → strings
		var lt = new LeapTable<Integer, String>();

		// Append entries (keys must be non-decreasing)
		for (int i = 1; i <= 10; i++) {
			lt.append(i, "Value-" + i);
		}

		System.out.println("\nInitial table:");
		printAll(lt);

		// Lookup examples
		System.out.println("\nLookup examples:");
		System.out.println("get(5)  = " + lt.get(5));
		System.out.println("get(99) = " + lt.get(99));
		System.out.println("floorEntry(7).value = " + lt.floorEntry(7).getValue());
		System.out.println("ceilEntry(7).value  = " + lt.ceilEntry(7).getValue());

		// Delete a few entries
		lt.remove(3);
		lt.remove(4);
		lt.remove(9);

		System.out.println("\nAfter deletions (logical tombstones):");
		printAll(lt);
		System.out.println("sizeLive()     = " + lt.sizeLive());
		System.out.println("sizePhysical() = " + lt.sizePhysical());

		// Range scan example
		System.out.println("\nRange scan [2, 7]:");
		lt.range(2, 7, (k, v) -> System.out.println("  " + k + " -> " + v));

		// Compaction (physically removes tombstones)
		System.out.println("\nCompacting...");
		lt.compact();

		System.out.println("After compaction:");
		printAll(lt);
		System.out.println("sizeLive()     = " + lt.sizeLive());
		System.out.println("sizePhysical() = " + lt.sizePhysical());

		// Append new keys (still monotone)
		lt.append(11, "Value-11");
		lt.append(12, "Value-12");
		lt.append(13, "Value-13");

		System.out.println("\nAfter further appends:");
		printAll(lt);
		
		System.out.println("\nPrinted with toString: " + lt.toString());

		System.out.println("\n=== Demo complete ===");
	}

	private static void printAll(LeapTable<Integer, String> lt) {
		System.out.println("Contents:");
		for (var e : lt) {
			System.out.println("  " + e.getKey() + " = " + e.getValue());
		}
	}
}
