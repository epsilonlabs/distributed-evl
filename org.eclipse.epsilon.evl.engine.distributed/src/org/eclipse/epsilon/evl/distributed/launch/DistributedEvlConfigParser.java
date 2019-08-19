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
import org.eclipse.epsilon.eol.cli.EolConfigParser;

/**
 * 
 *
 * @author Sina Madani
 * @since 1.6
 * @param <J>
 * @param <B>
 */
public class DistributedEvlConfigParser<R extends DistributedEvlRunConfiguration, B extends DistributedEvlRunConfiguration.Builder<R, B>> extends EolConfigParser<R, B> {

	public static void main(String... args) {
		new DistributedEvlConfigParser<>().parseAndRun(args);
	}
	
	private final String
		hostOpt = "host",
		basePathOpt = "basePath",
		sessionIdOpt = "session";
	
	@SuppressWarnings("unchecked")
	public DistributedEvlConfigParser() {
		this((B) new DistributedEvlRunConfiguration.Builder<>());
	}
	
	public DistributedEvlConfigParser(B builder) {
		super(builder);		
		options.addOption(Option.builder()
			.longOpt(hostOpt)
			.desc("Address of the JMS broker host")
			.hasArg()
			.build()
		).addOption(Option.builder("base")
			.longOpt(basePathOpt)
			.desc("Base directory to start looking for resources from")
			.hasArg()
			.build()
		).addOption(Option.builder("id")
			.longOpt(sessionIdOpt)
			.desc("Identifier for the execution session")
			.hasArg()
			.build()
		);
	}
	
	@Override
	public void parseArgs(String[] args) throws Exception {
		super.parseArgs(args);
		builder.host = cmdLine.getOptionValue(hostOpt);
		builder.basePath = cmdLine.getOptionValue(basePathOpt);
		builder.sessionID = tryParse(sessionIdOpt, builder.sessionID);
	}
}
