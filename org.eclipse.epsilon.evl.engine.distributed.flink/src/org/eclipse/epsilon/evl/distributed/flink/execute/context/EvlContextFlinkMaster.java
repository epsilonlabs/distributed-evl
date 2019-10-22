/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.execute.context;

import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextFlinkMaster extends EvlContextDistributedMaster {

	protected String outputPath;
	
	public EvlContextFlinkMaster(int localParallelism, int distributedParallelism, JobSplitter strategy, java.nio.file.Path outputPath) {
		super(localParallelism, distributedParallelism, strategy);
		this.outputPath = java.util.Objects.toString(outputPath, null);
	}
	
	public String getOutputPath() {
		return outputPath;
	}
	
	@Override
	public EvlModuleFlinkMaster getModule() {
		return (EvlModuleFlinkMaster) super.getModule();
	}
}
