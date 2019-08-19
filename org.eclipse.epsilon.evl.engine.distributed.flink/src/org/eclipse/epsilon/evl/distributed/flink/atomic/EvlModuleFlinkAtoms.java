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

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.execute.context.EvlContextFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.format.FlinkInputFormat;
import org.eclipse.epsilon.evl.distributed.strategy.AtomicJobSplitter;

/**
 * Data-parallel evaluation strategy which works over elements.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleFlinkAtoms extends EvlModuleFlinkMaster<SerializableEvlInputAtom> {

	public EvlModuleFlinkAtoms(EvlContextFlinkMaster context, AtomicJobSplitter strategy) {
		super(context, strategy);
	}

	@SuppressWarnings("unchecked")
	protected List<SerializableEvlInputAtom> getWorkerJobs() throws EolRuntimeException {
		return (List<SerializableEvlInputAtom>) jobSplitter.getWorkerJobs();
	}
	
	@Override
	protected DataSource<SerializableEvlInputAtom> getProcessingPipeline(ExecutionEnvironment execEnv) throws Exception {
		return execEnv
			.createInput(
				new FlinkInputFormat<>(getWorkerJobs()),
				TypeInformation.of(SerializableEvlInputAtom.class)
			);
	}
}
