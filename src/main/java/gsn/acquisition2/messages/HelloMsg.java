/**
* Global Sensor Networks (GSN) Source Code
* Copyright (c) 2006-2014, Ecole Polytechnique Federale de Lausanne (EPFL)
* 
* This file is part of GSN.
* 
* GSN is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
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
* File: src/gsn/acquisition2/messages/HelloMsg.java
*
* @author Ali Salehi
*
*/

package gsn.acquisition2.messages;

import gsn.beans.AddressBean;

public class HelloMsg extends AbstractMessage {

  private static final long serialVersionUID = -4418222946153175066L;

  private boolean continueOnError = false;
  
  private AddressBean wrapperDetails;
  
  private String requster = null;

  public boolean isContinueOnError() {
    return continueOnError;
  }

  public void setContinueOnError(boolean isContinueOnError) {
    this.continueOnError = isContinueOnError;
  }

  public AddressBean getWrapperDetails() {
    return wrapperDetails;
  }

  public void setWrapperDetails(AddressBean wrapperDetails) {
    this.wrapperDetails = wrapperDetails;
  }

  public HelloMsg(AddressBean wrapperDetails,String requester, boolean isContinueOnError) {
    this.wrapperDetails = wrapperDetails;
    this.continueOnError = isContinueOnError;
    this.requster = requester;
  }

  /**
   * Sets the continueOnError to True by default.
   */
  public HelloMsg(AddressBean wrapperDetails,String requester) {
    this.wrapperDetails = wrapperDetails;
    this.continueOnError = true;
    this.requster = requester;
  }

  public String getRequster() {
    return requster;
  }
}
