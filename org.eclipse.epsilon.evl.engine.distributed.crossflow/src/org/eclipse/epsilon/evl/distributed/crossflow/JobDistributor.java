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

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class JobDistributor extends JobDistributorBase {
	
	@Override
	public void consumeConfigTopic(Config config) throws Exception {
		for (java.io.Serializable job : workflow.getConfigConfigSource().masterContext.getModule().workerJobs) {
			sendToValidationDataQueue(new ValidationData(job));
		}
	}
}