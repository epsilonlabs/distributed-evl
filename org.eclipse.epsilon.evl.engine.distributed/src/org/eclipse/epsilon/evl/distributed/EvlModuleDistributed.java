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

import java.util.Iterator;
import java.util.Objects;
import org.eclipse.epsilon.eol.exceptions.EolRuntimeException;
import org.eclipse.epsilon.erl.execute.data.RuleAtom;
import org.eclipse.epsilon.evl.concurrent.atomic.EvlModuleParallelContextAtoms;
import org.eclipse.epsilon.evl.distributed.execute.context.EvlContextDistributed;

/**
 * 
 * @author Sina Madani
 * @since 1.6
 */
public abstract class EvlModuleDistributed extends EvlModuleParallelContextAtoms {
   
   public EvlModuleDistributed(EvlContextDistributed context) {
      super(Objects.requireNonNull(context));
   }
   
   @Override
   public EvlContextDistributed getContext() {
      return (EvlContextDistributed) super.getContext();
   }
   
   /**
    * Retrieves the model element at the specified index in the jobs list.
    * 
    * @param index The position of the model element in the jobs list. 
    * @return The model element.
    * @throws EolRuntimeException
    */
   public Object modelElementAtIndex(int index) throws EolRuntimeException {
      return getAllJobs().get(index).element;
   }
   
   /**
    * Finds the position of the specified model element in the jobs list.
    * 
    * @param element The model element to find from the jobs list.
    * @return The index of the model element if present, or a negative value if absent.
    * @throws EolRuntimeException
    */
   public int indexOfModelElement(Object element) throws EolRuntimeException {
      Iterator<? extends RuleAtom<?>> iter = getAllJobs().iterator();
      for (int i = 0; iter.hasNext(); i++) {
         if (iter.next().element == element) return i;
      }
      return -1;
   }
}
