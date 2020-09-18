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
import javax.jms.ConnectionFactory;
import javax.naming.NamingException;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.jms.EvlModuleJmsMaster;
import org.eclipse.epsilon.evl.distributed.jms.internal.ConnectionFactoryObtainer;
import org.eclipse.epsilon.evl.distributed.strategy.JobSplitter;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class EvlContextJmsMaster extends EvlContextDistributedMaster {

	protected final int sessionID;
	protected final String brokerHost;
	protected final boolean includeLocalWorker;
	protected ConnectionFactory connectionFactory;

	public EvlContextJmsMaster(int localParallelism, int expectedWorkers, JobSplitter splitter, String brokerHost, int sessionID, boolean localWorker) {
		super(localParallelism, expectedWorkers, splitter);
		this.sessionID = sessionID;
		this.brokerHost = URI.create(brokerHost).toString();
		this.includeLocalWorker = localWorker;
	}
	
	public String getBrokerHost() {
		return brokerHost;
	}
	
	public int getSessionId() {
		return this.sessionID;
	}
	
	public boolean hasLocalJmsWorker() {
		return includeLocalWorker;
	}
	
	public synchronized ConnectionFactory getConnectionFactory() throws NamingException {
		if (connectionFactory == null) {
			connectionFactory = ConnectionFactoryObtainer.get(brokerHost);
		}
		return connectionFactory;
	}
	
	@Override
	public synchronized void dispose() {
		super.dispose();
		if (connectionFactory instanceof AutoCloseable) {
			try {
				((AutoCloseable) connectionFactory).close();
			}
			catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		connectionFactory = null;
	}
	
	@Override
	public EvlModuleJmsMaster getModule() {
		return (EvlModuleJmsMaster) super.getModule();
	}
}
