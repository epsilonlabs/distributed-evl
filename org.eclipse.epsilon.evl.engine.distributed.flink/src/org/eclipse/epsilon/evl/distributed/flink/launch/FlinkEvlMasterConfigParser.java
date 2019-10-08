/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.flink.launch;

import org.eclipse.epsilon.evl.distributed.flink.EvlModuleFlinkMaster;
import org.eclipse.epsilon.evl.distributed.flink.execute.context.EvlContextFlinkMaster;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlMasterConfigParser;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 * @param <J>
 * @param <B>
 */
public class FlinkEvlMasterConfigParser<R extends FlinkEvlRunConfigurationMaster, B extends FlinkEvlRunConfigurationMaster.Builder<R, B>> extends DistributedEvlMasterConfigParser<R, B> {

	public static void main(String... args) {
		new FlinkEvlMasterConfigParser<>().parseAndRun(args);
	}
	
	@SuppressWarnings("unchecked")
	public FlinkEvlMasterConfigParser() {
		this((B) new FlinkEvlRunConfigurationMaster.Builder<>());
	}
	
	public FlinkEvlMasterConfigParser(B builder) {
		super(builder);
	}
	
	@Override
	public void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		EvlContextFlinkMaster context = new EvlContextFlinkMaster(builder.parallelism, builder.distributedParallelism, builder.getJobSplitter());
		builder.module = new EvlModuleFlinkMaster(context);
	}
}
