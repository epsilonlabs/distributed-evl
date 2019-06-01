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
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelElementTypeNotFoundException;
import org.eclipse.epsilon.eol.exceptions.models.EolModelNotFoundException;
import org.eclipse.epsilon.eol.execute.context.IEolContext;
import org.eclipse.epsilon.eol.types.EolModelElementType;
import org.eclipse.epsilon.evl.execute.UnsatisfiedConstraint;
import org.eclipse.epsilon.evl.execute.atoms.ConstraintContextAtom;

/**
 * Data unit to be used as inputs in distributed processing. No additional
 * information over the base {@linkplain SerializableEvlAtom} is required.
 * 
 * @author Sina Madani
 * @since 1.6
 */
public class SerializableEvlInputAtom extends SerializableEvlAtom {

	private static final long serialVersionUID = -5175915914665990769L;

	@Override
	protected SerializableEvlInputAtom clone() {
		return (SerializableEvlInputAtom) super.clone();
	}
	
	@Override
	public Object findElement(IEolContext context) throws EolRuntimeException {
		Object modelElement = super.findElement(context);
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
	
	public SerializableEvlResultAtom serializeUnsatisfiedConstraint(UnsatisfiedConstraint unsatisfiedConstraint) {
		SerializableEvlResultAtom outputAtom = new SerializableEvlResultAtom();
		outputAtom.contextName = this.contextName;
		outputAtom.modelName = this.modelName;
		outputAtom.constraintName = unsatisfiedConstraint.getConstraint().getName();
		outputAtom.modelElementID = this.modelElementID;
		outputAtom.message = unsatisfiedConstraint.getMessage();
		return outputAtom;
	}
	
	/**
	 * Transforms the given non-serializable jobs into their serializable forms.
	 * 
	 * @param atoms The ConstraintContext and element pairs.
	 * @return A Serializable List of {@link SerializableEvlInputAtom}, in deterministic order.
	 * @throws EolModelElementTypeNotFoundException If resolving any of the model elements fails.
	 * @throws EolModelNotFoundException 
	 */
	public static ArrayList<SerializableEvlInputAtom> serializeJobs(Collection<ConstraintContextAtom> atoms) throws EolModelElementTypeNotFoundException, EolModelNotFoundException {
		ArrayList<SerializableEvlInputAtom> serAtoms = new ArrayList<>(atoms.size());	
		for (ConstraintContextAtom cca : atoms) {
			EolModelElementType modelType = cca.unit.getType(cca.context);
			SerializableEvlInputAtom sa = new SerializableEvlInputAtom();
			sa.modelName = modelType.getModelName();
			sa.modelElementID = modelType.getModel().getElementId(cca.element);
			sa.contextName = cca.unit.getTypeName();
			serAtoms.add(sa);
		}
		return serAtoms;
	}
}
