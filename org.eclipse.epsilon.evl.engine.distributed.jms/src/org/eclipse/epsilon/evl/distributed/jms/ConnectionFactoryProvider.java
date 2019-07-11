/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.jms;

/**
 * For managing dependency on the JMS implementation.
 * This is made as a package-private interface to prevent instantiation and extension.
 *
 * @author Sina Madani
 * @since 1.6
 */
interface ConnectionFactoryProvider {
	static javax.jms.ConnectionFactory getDefault(String host) {
		return new org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory(host);
	}
}
