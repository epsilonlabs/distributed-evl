/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.execute.context;

import java.net.URI;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextJmsMaster extends EvlContextDistributedMaster {

	protected final int sessionID;
	protected final String brokerHost;

	public EvlContextJmsMaster(EvlContextJmsMaster other) {
		super(other);
		this.sessionID = other.sessionID;
		this.brokerHost = other.brokerHost;
	}

	public EvlContextJmsMaster(int localParallelism, int expectedWorkers, String brokerHost, int sessionID) {
		super(localParallelism, expectedWorkers);
		this.sessionID = sessionID;
		this.brokerHost = URI.create(brokerHost).toString();
	}
	
	public String getBrokerHost() {
		return brokerHost;
	}
	
	public int getSessionId() {
		return this.sessionID;
	}
}
