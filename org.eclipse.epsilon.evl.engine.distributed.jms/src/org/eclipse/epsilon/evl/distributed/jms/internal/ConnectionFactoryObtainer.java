/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms.internal;

import java.util.Hashtable;
import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * Abstracts away concrete implementation dependency on getting the ConnectionFactory.
 * @author Sina Madani
 * @since 1.6
 */
public final class ConnectionFactoryObtainer {
	private ConnectionFactoryObtainer() {}
	
	public static ConnectionFactory get(String host) throws NamingException {
		Hashtable<String, Object> env = new Hashtable<>(2);
		env.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
		env.put("java.naming.provider.url", host);
		InitialContext ic = new InitialContext(env);
		ConnectionFactory cf = (ConnectionFactory) ic.lookup("ConnectionFactory");
		return cf;
	}
}
