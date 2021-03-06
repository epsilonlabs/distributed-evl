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

import java.util.ArrayList;
import java.util.Collection;
import org.eclipse.epsilon.common.util.StringUtil;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelElementTypeNotFoundException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelNotFoundException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.eol.types.EolModelElementType;
import org.eclipse.epsilon.evl.IEvlModule;
import org.eclipse.epsilon.evl.dom.Constraint;
import org.eclipse.epsilon.evl.dom.ConstraintContext;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.atoms.EvlAtom;
import org.eclipse.epsilon.evl.execute.context.IEvlContext;

/**
 * Data unit to be used as inputs in distributed processing. No additional
 * information over the base {@linkplain SerializableEvlAtom} is required.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class SerializableEvlInputAtom extends SerializableEvlAtom {

	private static final long serialVersionUID = -5643807454658114529L;

	@Override
	protected SerializableEvlInputAtom clone() {
		return (SerializableEvlInputAtom) super.clone();
	}
	
	@Override
	public Object getModelElement(IEolContext context) throws EolRuntimeException {
		Object modelElement = super.getModelElement(context);
		if (modelElement == null) {
			throw new EolRuntimeException(
				"Could not find model element with ID "+modelElementID+
				(modelName != null && modelName.trim().length() > 0 ? 
					" in model "+modelName : ""
				)
				+" in context of "+contextName
			);
		}
		return modelElement;
	}
	
	public void execute(IEvlModule module) throws EolRuntimeException {
		IEvlContext context = module.getContext();
		Object modelElement = getModelElement(context);
		module.getConstraintContext(contextName).execute(modelElement, context);
	}
	
	public SerializableEvlResultAtom serializeUnsatisfiedConstraint(UnsatisfiedConstraint unsatisfiedConstraint) {
		SerializableEvlResultAtom outputAtom = new SerializableEvlResultAtom();
		outputAtom.contextName = StringUtil.isEmpty(contextName) ?
			unsatisfiedConstraint.getConstraint().getConstraintContext().getTypeName() : this.contextName;
		outputAtom.modelName = this.modelName;
		outputAtom.constraintName = StringUtil.isEmpty(constraintName) ?
			unsatisfiedConstraint.getConstraint().getName() : this.constraintName;
		outputAtom.modelElementID = this.modelElementID;
		outputAtom.message = unsatisfiedConstraint.getMessage();
		return outputAtom;
	}

	/**
	 * Transforms the given non-serializable jobs into their serializable forms.
	 * 
	 * @param atoms The rule and element pairs.
	 * @return A Serializable List of {@link SerializableEvlInputAtom}, in deterministic order.
	 * @throws EolModelElementTypeNotFoundException If resolving any of the model elements fails.
	 * @throws EolModelNotFoundException 
	 */
	public static ArrayList<SerializableEvlInputAtom> serializeJobs(Collection<? extends EvlAtom<?>> atoms, IEvlContext context) throws EolModelElementTypeNotFoundException, EolModelNotFoundException {
		ArrayList<SerializableEvlInputAtom> serAtoms = new ArrayList<>(atoms.size());	
		for (EvlAtom<?> atom : atoms) {
			serAtoms.add(serializeJob(atom, context));
		}
		return serAtoms;
	}
	
	public static SerializableEvlInputAtom serializeJob(EvlAtom<?> atom, IEvlContext context) throws EolModelElementTypeNotFoundException, EolModelNotFoundException {
		boolean isConstraint = atom.rule instanceof Constraint;
		ConstraintContext cc = isConstraint ? ((Constraint) atom.rule).getConstraintContext() : (ConstraintContext) atom.rule;
		EolModelElementType modelType = cc.getType(context);
		SerializableEvlInputAtom sa = new SerializableEvlInputAtom();
		sa.modelName = modelType.getModelName();
		sa.modelElementID = modelType.getModel().getElementId(atom.element);
		sa.contextName = cc.getTypeName();
		if (isConstraint) sa.constraintName = ((Constraint) atom.rule).getName();
		return sa;
	}
}
