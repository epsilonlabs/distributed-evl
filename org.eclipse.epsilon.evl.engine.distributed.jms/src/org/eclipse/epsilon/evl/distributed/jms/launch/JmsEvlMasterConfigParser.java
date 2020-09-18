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
	
	private final String localJmsWorkerOpt = "localWorker";
	
	@SuppressWarnings("unchecked")
	public JmsEvlMasterConfigParser() {
		this((B) new JmsEvlRunConfigurationMaster.Builder<>());
	}
	
	public JmsEvlMasterConfigParser(B builder) {
		super(builder);
		options.addOption(localJmsWorkerOpt, "Whether the master should spawn its own JMS worker");
	}
	
	@Override
	public void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		builder.includeLocalWorker = cmdLine.hasOption(localJmsWorkerOpt);
		if (builder.host == null || builder.host.isEmpty()) {
			builder.host = "tcp://localhost:61616";
		}
		else {
			if (!builder.host.contains("://")) builder.host = "tcp://"+builder.host;
			URI hostUri = new URI(builder.host);
			builder.host = hostUri.toString();
			if (hostUri.getPort() <= 0) builder.host += ":61616";
		}
	}
}
