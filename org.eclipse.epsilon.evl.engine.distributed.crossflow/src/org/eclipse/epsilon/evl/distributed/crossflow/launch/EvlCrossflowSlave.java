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
import org.eclipse.scava.crossflow.runtime.Mode;

public class EvlCrossflowSlave {

	public static void main(String... args) throws Exception {
		DistributedEVL crossflow = new DistributedEVL(Mode.WORKER);
		crossflow.setInstanceId("DistributedEVL");
		if (args.length > 0 && !args[0].isEmpty()) {
			crossflow.setMaster(args[0]);
		}
		crossflow.run();
	}

}
