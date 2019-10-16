/*********************************************************************
 * Copyright (c) 2018 The University of York.
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
import org.eclipse.epsilon.eol.models.IModel;
import org.eclipse.epsilon.evl.distributed.EvlModuleDistributed;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributed;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;

/**
 * Serializable representation of an {@linkplain UnsatisfiedConstraint}.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class SerializableEvlResultAtom extends SerializableEvlAtom implements SerializableEvlResult {
	
	private static final long serialVersionUID = 8038574676833172172L;
	
	
	public String message;
	
	@Override
	protected SerializableEvlResultAtom clone() {
		SerializableEvlResultAtom clone = (SerializableEvlResultAtom) super.clone();
		clone.message = this.message != null ? ""+this.message : null;
		return clone;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), message);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		SerializableEvlResultAtom other = (SerializableEvlResultAtom) obj;
		return
			Objects.equals(this.message, other.message);
	}

	@Override
	public String toString() {
		String start = super.toString();
		start = start.substring(0, start.length() - 1);
		return start + ", message=" + message+"}";
	}
	
	public static SerializableEvlResult serialize(UnsatisfiedConstraint uc, EvlContextDistributed context) {
		if (uc == null) return null;
		SerializableEvlResultAtom outputAtom = new SerializableEvlResultAtom();
		Object modelElement = uc.getInstance();
		IModel owningModel = context.getModelRepository().getOwningModel(modelElement);
		outputAtom.contextName = uc.getConstraint().getConstraintContext().getTypeName();
		outputAtom.modelName = owningModel.getName();
		outputAtom.modelElementID = owningModel.getElementId(modelElement);
		outputAtom.constraintName = uc.getConstraint().getName();
		outputAtom.message = uc.getMessage();
		return outputAtom;
	}

	@Override
	public Constraint getConstraint(EvlModuleDistributed module) throws EolRuntimeException {
		return module.getConstraint(constraintName, contextName, null, null);
	}

	@Override
	public String getMessage(EvlModuleDistributed module) {
		return message;
	}

	@Override
	public Object getModelElement(EvlModuleDistributed module) throws EolRuntimeException {
		return getModelElement(module.getContext());
	}
}
