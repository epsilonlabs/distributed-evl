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

import org.eclipse.epsilon.evl.distributed.flink.atomic.EvlModuleFlinkAtoms;
import org.eclipse.epsilon.evl.distributed.flink.batch.EvlModuleFlinkSubset;
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
		if (builder.batchFactor >= 0) {
			builder.module = new EvlModuleFlinkSubset(builder.distributedParallelism, builder.batchFactor);
		}
		else {
			builder.module = new EvlModuleFlinkAtoms(builder.distributedParallelism, builder.shuffle);
		}
	}
}
