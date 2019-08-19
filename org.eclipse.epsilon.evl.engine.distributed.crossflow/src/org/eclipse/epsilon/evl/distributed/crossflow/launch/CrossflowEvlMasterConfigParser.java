/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow.launch;

import org.apache.commons.cli.Option;
import org.eclipse.epsilon.evl.distributed.crossflow.atomic.EvlModuleCrossflowMasterAtomic;
import org.eclipse.epsilon.evl.distributed.crossflow.batch.EvlModuleCrossflowMasterBatch;
import org.eclipse.epsilon.evl.distributed.crossflow.execute.context.EvlContextCrossflowMaster;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlMasterConfigParser;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 * @param <J>
 * @param <B>
 */
public class CrossflowEvlMasterConfigParser<R extends CrossflowEvlRunConfigurationMaster, B extends CrossflowEvlRunConfigurationMaster.Builder<R, B>> extends DistributedEvlMasterConfigParser<R, B> {

	private final String instanceIdOpt = "instanceID";
	
	public static void main(String... args) {
		new CrossflowEvlMasterConfigParser<>().parseAndRun(args);
	}
	
	@SuppressWarnings("unchecked")
	public CrossflowEvlMasterConfigParser() {
		this((B) new CrossflowEvlRunConfigurationMaster.Builder<>());
	}
	
	public CrossflowEvlMasterConfigParser(B builder) {
		super(builder);
		options.addOption(Option.builder("id")
			.longOpt(instanceIdOpt)
			.hasArg()
			.desc("Instance ID for Crossflow")
			.build()
		);
	}
	
	@Override
	public void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		if (cmdLine.hasOption(instanceIdOpt)) {
			builder.instanceID = cmdLine.getOptionValue(instanceIdOpt);
		}
		EvlContextCrossflowMaster context = new EvlContextCrossflowMaster(builder.parallelism, builder.distributedParallelism, builder.instanceID);
		if (builder.batchFactor > 0) {
			builder.module = new EvlModuleCrossflowMasterBatch(context, getBatchStrategy(context));
		}
		else {
			builder.module = new EvlModuleCrossflowMasterAtomic(context, getAtomicStrategy(context));
		}
	}
}
