/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.format;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.flink.util.SplittableIterator;

/**
 * Partitions fixed size collections for parallel processing.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class ParallelFlinkIterator<T extends java.io.Serializable> extends SplittableIterator<T> {

	private static final long serialVersionUID = 3879082897443913401L;

	protected final List<T> source;
	private final int sourceSize;
	protected transient final Iterator<T> iterator;
	
	public ParallelFlinkIterator(List<? extends T> source) {
		this.iterator = (this.source = Collections.unmodifiableList(source)).iterator();
		this.sourceSize = source.size();
	}
	
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public T next() {
		return iterator.next();
	}

	@Override
	public int getMaximumNumberOfSplits() {
		return sourceSize;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<T>[] split(int numPartitions) {	
		if (sourceSize <= 0) {
			return new Iterator[]{};
		}
		else if (numPartitions < 2) {
			return new Iterator[] {source.iterator()};
		}
		else if (numPartitions >= sourceSize) {
			return source.stream()
				.map(Collections::singleton)
				.map(Iterable::iterator)
				.toArray(Iterator[]::new);
		}
		
		final int fullChunks = sourceSize / numPartitions;

		return IntStream.range(0, numPartitions)
			.mapToObj(i -> source.subList(
					i * fullChunks, 
					i == (numPartitions - 1) ? sourceSize : (i + 1) * fullChunks
				)
				.iterator()
			)
			.toArray(Iterator[]::new);
	}
	
}
