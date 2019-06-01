/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.atomic;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.format.FlinkInputFormat;

/**
 * Data-parallel evaluation strategy which works over elements.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleFlinkAtoms extends EvlModuleFlinkMaster<SerializableEvlInputAtom> {

	protected final boolean shuffle;
	
	public EvlModuleFlinkAtoms() {
		this(-1);
	}
	
	public EvlModuleFlinkAtoms(int parallelism) {
		this(parallelism, false);
	}
	
	public EvlModuleFlinkAtoms(int parallelism, boolean shuffle) {
		super(parallelism);
		this.shuffle = shuffle;
	}

	@Override
	protected DataSource<SerializableEvlInputAtom> getProcessingPipeline(ExecutionEnvironment execEnv) throws Exception {
		return execEnv
			.createInput(
				new FlinkInputFormat<>(new AtomicJobSplitter(0, shuffle).getWorkerJobs()),
				TypeInformation.of(SerializableEvlInputAtom.class)
			);
	}
}
