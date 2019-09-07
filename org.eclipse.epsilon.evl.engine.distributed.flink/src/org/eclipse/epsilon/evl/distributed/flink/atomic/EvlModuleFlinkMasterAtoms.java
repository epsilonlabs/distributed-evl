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

import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.execute.context.EvlContextFlinkMaster;
import org.eclipse.epsilon.evl.distributed.strategy.ContextAtomJobSplitter;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;

/**
 * Data-parallel evaluation strategy which works over elements.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleFlinkMasterAtoms extends EvlModuleFlinkMaster {

	public EvlModuleFlinkMasterAtoms(EvlContextFlinkMaster context, ContextAtomJobSplitter strategy) {
		this(context, (JobSplitter<?, SerializableEvlInputAtom>) strategy);
	}

	EvlModuleFlinkMasterAtoms(EvlContextFlinkMaster context, JobSplitter<?, SerializableEvlInputAtom> strategy) {
		super(context, strategy);
	}
}
