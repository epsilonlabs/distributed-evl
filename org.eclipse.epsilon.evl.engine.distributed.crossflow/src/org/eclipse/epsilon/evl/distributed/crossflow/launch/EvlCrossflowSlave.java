/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow.launch;

import org.eclipse.epsilon.evl.distributed.crossflow.DistributedEVL;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributed;
import org.eclipse.scava.crossflow.runtime.Mode;

public class EvlCrossflowSlave {
	
	public static void main(String... args) throws Exception {
		DistributedEVL crossflow = new DistributedEVL(Mode.WORKER);
		System.setProperty(EvlContextDistributed.BASE_PATH_SYSTEM_PROPERTY, args.length > 0 ? args[0] : null);
		if (args.length > 1 && !args[1].isEmpty()) {
			crossflow.setMaster(args[1]);
		}
		crossflow.setInstanceId("DistributedEVL");
		crossflow.run();
	}

}
