/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.strategy;

import java.util.Collection;
import java.util.List;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedMaster;
import org.eclipse.epsilon.evl.distributed.execute.data.SerializableEvlInputAtom;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintContextAtom;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class ContextAtomJobSplitter extends JobSplitter<ConstraintContextAtom, SerializableEvlInputAtom> {
	
	public ContextAtomJobSplitter(EvlContextDistributedMaster context, double masterProportion, boolean shuffle) {
		super(context, masterProportion, shuffle);
	}

	@Override
	protected List<ConstraintContextAtom> getAllJobs() throws EolRuntimeException {
		return context.getModule().getAllJobs();
	}
	
	@Override
	protected Collection<SerializableEvlInputAtom> convertToWorkerJobs(Collection<ConstraintContextAtom> jobs) throws EolRuntimeException {
		return SerializableEvlInputAtom.serializeJobs(masterJobs, context);
	}
}