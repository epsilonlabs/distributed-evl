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

import java.net.URI;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;
import org.eclipse.epsilon.evl.distributed.jms.execute.context.EvlContextJmsMaster;
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
		
		if (builder.host == null || builder.host.isEmpty()) {
			builder.host = "tcp://localhost:61616";
		}
		else {
			if (!builder.host.contains("://")) builder.host = "tcp://"+builder.host;
			URI hostUri = new URI(builder.host);
			builder.host = hostUri.toString();
			if (hostUri.getPort() <= 0) builder.host += ":61616";
		}
		
		EvlContextJmsMaster context = new EvlContextJmsMaster(builder.parallelism, builder.distributedParallelism, builder.getJobSplitter(), builder.host, builder.sessionID);
		builder.module = new EvlModuleJmsMaster(context);
	}
}
