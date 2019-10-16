/*********************************************************************
 * Copyright (c) 2019 The University of York.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
**********************************************************************/
package org.eclipse.epsilon.evl.distributed.execute.data;

import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * Marker interface for serialized {@link UnsatisfiedConstraint}s.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public interface SerializableEvlResult extends java.io.Serializable {
	
	Constraint getConstraint(EvlModuleDistributed module) throws EolRuntimeException;

	Object getModelElement(EvlModuleDistributed module) throws EolRuntimeException;

	String getMessage(EvlModuleDistributed module) throws EolRuntimeException;
	
	/**
	 * Provides a reference to an UnsatisfiedConstraint with this atom's values used for resolution.
	 * 
	 * @param module The IEvlModule used to resolve this atom.
	 * @return An {@linkplain UnsatisfiedConstraint} with the properties not resolved (yet).
	 */
	default LazyUnsatisfiedConstraint deserializeLazy(EvlModuleDistributed module) {
		return new LazyUnsatisfiedConstraint(this, module);
	}
	
	// TODO: support fixes and 'extras'
	/**
	 * Transforms the serialized UnsatisfiedConstraint into a native UnsatisfiedConstraint.
	 * 
	 * @param module The IEvlModule used to resolve this atom.
	 * @return The derived {@link UnsatisfiedConstraint} with its properties populated.
	 * @throws EolRuntimeException If the constraint or model element could not be found.
	 */
	default UnsatisfiedConstraint deserializeEager(EvlModuleDistributed module) throws EolRuntimeException {
		UnsatisfiedConstraint uc = new UnsatisfiedConstraint();
		uc.setInstance(getModelElement(module));
		uc.setMessage(getMessage(module));
		uc.setConstraint(getConstraint(module));
		return uc;
	}
}
