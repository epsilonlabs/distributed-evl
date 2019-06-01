/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.crossflow;

import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributedMaster;
import org.eclipse.scava.crossflow.runtime.Mode;

public class EvlModuleCrossflowMaster extends EvlModuleDistributedMaster {

	public EvlModuleCrossflowMaster() {
		super(-1);
	}

	@Override
	protected void checkConstraints() throws EolRuntimeException {
		try {
			DistributedEVL crossflow = new DistributedEVL(Mode.MASTER_BARE);
			crossflow.setInstanceId("DistributedEVL");
			//
			crossflow.getConfigConfigSource().masterModule = this;
			//
			crossflow.run(5000L);
			
			crossflow.awaitTermination();

			//
			crossflow.getResultSink();
		}
		catch (Exception ex) {
			throw new EolRuntimeException(ex);
		}
	}
	
	@Override
	protected boolean deserializeResults(Object response) throws EolRuntimeException {
		boolean result = super.deserializeResults(response);
		if (!result && response instanceof ValidationResult) {
			result = super.deserializeResults(((ValidationResult) response).getAtoms());
		}
		return result;
	}
}
