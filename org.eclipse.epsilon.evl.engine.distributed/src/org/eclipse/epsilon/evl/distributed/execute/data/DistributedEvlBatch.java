/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.execute.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Simple over-the-wire input for telling each node the start and end indexes
 * of their batch to process.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class DistributedEvlBatch implements java.io.Serializable, Cloneable {

	private static final long serialVersionUID = 5398211400355108382L;
	
	public int from, to;
	
	public DistributedEvlBatch() {}
	
	public DistributedEvlBatch(int from, int to) {
		this.from = from;
		this.to = to;
	}
	
	@Override
	protected DistributedEvlBatch clone() {
		DistributedEvlBatch clone;
		try {
			clone = (DistributedEvlBatch) super.clone();
		}
		catch (CloneNotSupportedException cnsx) {
			throw new UnsupportedOperationException(cnsx);
		}
		clone.from = this.from;
		clone.to = this.to;
		return clone;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(from, to);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!(obj instanceof DistributedEvlBatch)) return false;
		DistributedEvlBatch other = (DistributedEvlBatch) obj;
		return this.from == other.from && this.to == other.to;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName()+": from="+from+", to="+to;
	}
	
	/**
	 * Provides a List of indices based on the desired split size.
	 * 
	 * @param totalJobs The size of the source List being split
	 * @param chunks The range (i.e. <code>to - from</code>) of each batch.
	 * The last batch may be smaller than this but the other batches are guaranteed
	 * to be of this size.
	 * @return A Serializable List of indexes with {@code totalJobs/batches} increments.
	 */
	public static List<DistributedEvlBatch> getBatches(int totalJobs, int chunks) {
		final int
			modulo = totalJobs % chunks,
			division = totalJobs / chunks,
			maxBatches = modulo > 0 ? 1 + (totalJobs / chunks) : division;
		
		ArrayList<DistributedEvlBatch> resultList = new ArrayList<>(maxBatches);

		for (int prev = 0, curr = chunks; curr <= totalJobs; curr += chunks) {
			resultList.add(new DistributedEvlBatch(prev, prev = curr));
		}
		
		if (maxBatches > 0 && modulo > 0) {
			resultList.add(new DistributedEvlBatch(totalJobs - modulo, totalJobs));
		}
		
		assert resultList.size() == maxBatches;
		return resultList;
	}

	public <T> List<T> splitToList(T[] arr) {
		return Arrays.asList(split(arr));
	}
	public <T> T[] split(T[] arr) {
		return Arrays.copyOfRange(arr, from, to);
	}
	
	/**
	 * Splits the given list based on this class's indices.
	 * @param <T> The type of the List
	 * @param list The list to call {@link List#subList(int, int)} on
	 * @return The split list.
	 */
	public <T> List<T> split(List<T> list) {
		return list.subList(from, to);
	}
}
