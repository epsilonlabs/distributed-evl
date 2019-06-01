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

import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.java.io.ParallelIteratorInputFormat;

/**
 * Convenience class to be used as the source for Flink execution.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class FlinkInputFormat<T extends Serializable> extends ParallelIteratorInputFormat<T> {

	private static final long serialVersionUID = 5490512472038698409L;

	public FlinkInputFormat(List<? extends T> jobs) {
		super(new ParallelFlinkIterator<>(jobs));
	}

}
