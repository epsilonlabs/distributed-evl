/*********************************************************************
 * Copyright (c) 2018 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed;

import java.util.Collection;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributedSlave;
import org.eclipse.epsilon.evl.distributed.execute.data.*;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * A worker EVL module, intended to be spawned during distributed processing.
 * The execution of this module is performed element by element rather than in
 * bulk. That is, the equivalent of the {@link #checkConstraints()} method is
 * {@link #evaluateElement(SerializableEvlInputAtom)}, which is called by the
 * distributed processing framework.
 * 
 * @see {@link EvlModuleDistributedMaster}
 * @author Sina Madani
 * @since 1.6
 */
public class EvlModuleDistributedSlave extends EvlModuleDistributed {
	
	public EvlModuleDistributedSlave() {
		this(null);
	}
	
	public EvlModuleDistributedSlave(EvlContextDistributedSlave context) {
		super(context != null ? context : new EvlContextDistributedSlave());
	}
	
	@Override
	public EvlContextDistributedSlave getContext() {
		return (EvlContextDistributedSlave) super.getContext();
	}
	
	@Override
	public Collection<UnsatisfiedConstraint> executeImpl() throws EolRuntimeException {
		throw new UnsupportedOperationException("This method should only be called by the master!");
	}
	
	@Override
	protected void checkConstraints() throws EolRuntimeException {
		throw new IllegalStateException("This method should only be called by the master!");
	}
	
	// METHOD VISIBILITY
	
	@Override
	public void prepareExecution() throws EolRuntimeException {
		super.prepareExecution();
	}
	
	@Override
	public void postExecution() throws EolRuntimeException {
		// Do nothing
	}
}
