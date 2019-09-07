/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.batch;

import org.eclipse.epsilon.erl.execute.data.JobBatch;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;
import org.eclipse.epsilon.evl.distributed.jms.execute.context.EvlContextJmsMaster;
import org.eclipse.epsilon.evl.distributed.strategy.BatchJobSplitter;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintContextAtom;

/**
 * Batch-based approach, requiring only indices of the deterministic
 * jobs created from ConstraintContext and element pairs.
 * 
 * @see ConstraintContextAtom
 * @see JobBatch
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleJmsMasterBatch extends EvlModuleJmsMaster {
	
	public EvlModuleJmsMasterBatch(EvlContextJmsMaster context, BatchJobSplitter strategy) {
		super(context, strategy);
	}
}
