/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.DistributedEvlBatch;

public class JobDistributor extends JobDistributorBase {
	
	@Override
	public void consumeConfigTopic(Config config) throws Exception {
		
		final EvlModuleDistributedMaster module = workflow.getConfigConfigSource().masterModule;
		
		for (DistributedEvlBatch batch : module.getBatches(0.000975d)) {
			sendToValidationDataQueue(new ValidationData(batch));
		}
	}
}