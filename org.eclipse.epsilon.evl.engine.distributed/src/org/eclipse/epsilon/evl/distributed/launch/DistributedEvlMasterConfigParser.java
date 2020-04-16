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

public class DistributedEvlMasterConfigParser<R extends DistributedEvlRunConfigurationMaster, B extends DistributedEvlRunConfigurationMaster.Builder<R, B>> extends DistributedEvlConfigParser<R, B> {
	
	private final String
		distributedParallelismOpt = DistributedEvlRunConfigurationMaster.DISTRIBUTED_PARALLELISM,
		shuffleOpt = "no-shuffle",
		batchesOpt = "batches",
		masterProportionOpt = "masterProportion",
		localStandaloneOpt = "localStandalone";
	
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
		).addOption(Option.builder("local")
			.longOpt(localStandaloneOpt)
			.desc("Run this program locally, creating the workers and broker on this machine.")
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
		builder.localStandalone = cmdLine.hasOption(localStandaloneOpt);
	}
	
}
