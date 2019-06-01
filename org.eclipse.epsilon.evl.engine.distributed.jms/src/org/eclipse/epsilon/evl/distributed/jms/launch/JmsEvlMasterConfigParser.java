/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.launch;

import org.eclipse.epsilon.evl.distributed.jms.atomic.EvlModuleJmsMasterAtomic;
import org.eclipse.epsilon.evl.distributed.jms.batch.EvlModuleJmsMasterBatch;
import org.eclipse.epsilon.evl.distributed.launch.DistributedEvlMasterConfigParser;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 * @param <J>
 * @param <B>
 */
public class JmsEvlMasterConfigParser<R extends JmsEvlRunConfigurationMaster, B extends JmsEvlRunConfigurationMaster.Builder<R, B>> extends DistributedEvlMasterConfigParser<R, B> {

	public static void main(String... args) {
		new JmsEvlMasterConfigParser<>().parseAndRun(args);
	}
	
	@SuppressWarnings("unchecked")
	public JmsEvlMasterConfigParser() {
		this((B) new JmsEvlRunConfigurationMaster.Builder<>());
	}
	
	public JmsEvlMasterConfigParser(B builder) {
		super(builder);		
	}
	
	@Override
	public void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		if (builder.batchFactor > 0) {
			builder.module = new EvlModuleJmsMasterBatch(
				builder.distributedParallelism, builder.masterProportion, builder.batchFactor, builder.shuffle, builder.host, builder.sessionID
			);
		}
		else {
			builder.module = new EvlModuleJmsMasterAtomic(
				builder.distributedParallelism, builder.masterProportion, builder.shuffle, builder.host, builder.sessionID
			);
		}
	}
}
