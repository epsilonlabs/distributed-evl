/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.launch;

import org.apache.commons.cli.Option;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.strategy.*;

public class DistributedEvlMasterConfigParser<R extends DistributedEvlRunConfigurationMaster, B extends DistributedEvlRunConfigurationMaster.Builder<R, B>> extends DistributedEvlConfigParser<R, B> {
	
	private final String
		distributedParallelismOpt = "distributedParallelism",
		shuffleOpt = "no-shuffle",
		batchesOpt = "batches",
		masterProportionOpt = "masterProportion";
	
	public DistributedEvlMasterConfigParser(B builder) {
		super(builder);
		options.addOption(Option.builder("bf")
			.longOpt(batchesOpt)
			.desc("Granularity of job batches if between 0 and 1, or the batch size if greater than 1."
				+ "Either way sets the module to batch-based."
			)
			.hasArg()
			.build()
		).addOption(Option.builder("noshuffle")
			.longOpt(shuffleOpt)
			.desc("Disables order randomisation of jobs")
			.build()
		).addOption(Option.builder("workers")
			.longOpt(distributedParallelismOpt)
			.desc("The expected number of slave workers")
			.hasArg()
			.build()
		).addOption(Option.builder("mp")
			.longOpt(masterProportionOpt)
			.desc("Fraction of jobs to assign to the master (between 0 and 1)")
			.hasArg()
			.build()
		);
	}
	
	@Override
	public void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		builder.shuffle = !cmdLine.hasOption(shuffleOpt);
		builder.distributedParallelism = tryParse(distributedParallelismOpt, builder.distributedParallelism);
		builder.batchFactor = tryParse(batchesOpt, builder.batchFactor);
		builder.masterProportion = tryParse(masterProportionOpt, builder.masterProportion);
	}
	
	protected BatchJobSplitter getBatchStrategy(EvlContextDistributedMaster context) {
		return new BatchJobSplitter(context, builder.masterProportion, builder.shuffle, builder.batchFactor);
	}
	
	protected AnnotationConstraintAtomJobSplitter getAnnotationStrategy(EvlContextDistributedMaster context) {
		return new AnnotationConstraintAtomJobSplitter(context, builder.shuffle);
	}
	
	protected ContextAtomJobSplitter getAtomicStrategy(EvlContextDistributedMaster context) {
		return new ContextAtomJobSplitter(context, builder.masterProportion, builder.shuffle);
	}
}
