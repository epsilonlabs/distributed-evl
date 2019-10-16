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

import java.util.Objects;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * An @link {@link UnsatisfiedConstraint} which lazily resolves the constraint and model element
 * upon first request.
 *
 * @author Sina Madani
 * @since 1.6
 */
public class LazyUnsatisfiedConstraint extends UnsatisfiedConstraint {

	protected final SerializableEvlResult proxy;
	protected transient EvlModuleDistributed module;
	
	public LazyUnsatisfiedConstraint(SerializableEvlResult proxy, EvlModuleDistributed module) {
		Objects.requireNonNull(this.proxy = proxy);
		this.module = module;
	}
	
	public SerializableEvlResult getProxy() {
		return proxy;
	}

	public void resolve() {
		getMessage();
		getConstraint();
		getInstance();
		getFixes();
		getExtras();
	}
	
	@Override
	public String getMessage() {
		if (message == null) try {
			message = proxy.getMessage(module);
		}
		catch (EolRuntimeException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
		return message;
	}
	
	@Override
	public Constraint getConstraint() {
		if (constraint == null) try {
			constraint = proxy.getConstraint(module);
		}
		catch (EolRuntimeException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
		return constraint;
	}

	@Override
	public Object getInstance() {
		if (instance == null) try {
			instance = proxy.getModelElement(module);
		}
		catch (EolRuntimeException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
		return instance;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(proxy);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj instanceof LazyUnsatisfiedConstraint) {
			return Objects.equals(this.proxy, ((LazyUnsatisfiedConstraint) obj).proxy);
		}
		return super.equals(obj);
	}
}
