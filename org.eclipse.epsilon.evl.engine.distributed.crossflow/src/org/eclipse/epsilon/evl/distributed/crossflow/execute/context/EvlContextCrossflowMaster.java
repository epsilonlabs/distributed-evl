/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow.execute.context;

import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;

public class EvlContextCrossflowMaster extends EvlContextDistributedMaster {
	
	protected String instanceID;
	
	public EvlContextCrossflowMaster(int distributedParallelism, String instanceId) {
		super(distributedParallelism);
		this.instanceID = instanceId;
	}

	public EvlContextCrossflowMaster(EvlContextCrossflowMaster other) {
		super(other);
		this.instanceID = other.instanceID;
	}

	public EvlContextCrossflowMaster(int localParallelism, int distributedParallelism, String instanceId) {
		super(localParallelism, distributedParallelism);
		this.instanceID = instanceId;
	}

	public String getInstanceId() {
		return this.instanceID;
	}
	
}
