/**
* Global Sensor Networks (GSN) Source Code
* Copyright (c) 2006-2014, Ecole Polytechnique Federale de Lausanne (EPFL)
* 
* This file is part of GSN.
* 
* GSN is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 2 of the License, or
* (at your option) any later version.
* 
* GSN is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
* 
* You should have received a copy of the GNU General Public License
* along with GSN.  If not, see <http://www.gnu.org/licenses/>.
* 
* File: src/gsn/utils/protocols/AbstractHCIQuery.java
*
* @author Jerome Rousselot
* @author Ali Salehi
*
*/

package gsn.utils.protocols;

import java.util.Vector;

/**
 *  This class represents a query of a host controller interface protocol.
 *  Such a protocol can be used to communicate with a node connected
 *  to a computer through a hardware interface (bluetooth, usb, serial port,
 *  parallel port...) using higher-level abstractions.
 *  This means that instead of writing the appropriate bytes to a port to
 *  get a node ID, you can send the AbstractHCIQuery "getID".
 *  This makes the code easier to read and to maintain.
 */
public abstract class AbstractHCIQuery {
	
	public String QUERY_NAME;
	public static final int NO_WAIT_TIME = 0;
	private String queryDescription;
	private String[] paramsDescriptions;
	
	public AbstractHCIQuery(String name, String queryDescription, String[] paramsDescriptions) {
		QUERY_NAME = name;
		this.queryDescription = queryDescription;
		this.paramsDescriptions = paramsDescriptions;
	}
	/*
	 * This method returns the name of this query.
	 * 
	 */
	public String getName() {
		return QUERY_NAME;
	}
	/*
	 * This method takes a Vector of arguments as input and
	 * produces the raw data to be sent to the wrapper.
	 * Note that elements of a Vector are ordered, and that
	 * the Vector can be null.
	 * @param params A vector of Object containing the required
	 * parameters for this query. This should be described in
	 * the implementation.
	 * 
	 */
	public abstract byte[] buildRawQuery(Vector<Object> params);
	
	/*
	 * This method must return one String per parameter, matching
	 * the order in which they should be provided to the method
	 * buildRawQuery. Each String should describe for what this
	 * parameter is used for, whether or not it is optional, and
	 * what is its appropriate format.
	 * Keep in mind that this text is provided as a help for the user.
	 */
	public String[] getParamsDescriptions() {
		return paramsDescriptions;
	}
	
	/*
	 * This method must return a textual description of what
	 * this query does, and any interesting information for
	 * the user.
	 */
	public String getQueryDescription() {
		return queryDescription;
	}
	/*
	 * This method tells us whether we should wait for an answer
	 * from the mote if we send a query with these parameters.
	 * @param params A vector of Object containing the required
	 * parameters for this query. This should be described in
	 * the implementation.
	 *
	 */
	public abstract boolean needsAnswer(Vector<Object> params);
	
	/*
	 * This method tells us how much time we should wait for an
	 *  answer from the mote if we send a query with these 
	 *  parameters.
	 * @param params A vector of Object containing the required
	 * parameters for this query. This should be described in
	 * the implementation.
	 *
	 */
	public abstract int getWaitTime(Vector<Object> params);

	public abstract Object[] getAnswers(byte[] rawAnswer);
	
}
