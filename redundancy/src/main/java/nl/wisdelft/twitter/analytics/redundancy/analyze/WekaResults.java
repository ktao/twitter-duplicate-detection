/*********************************************************
*  Copyright (c) 2010 by Web Information Systems (WIS) Group.
*  Fabian Abel, http://fabianabel.de/
*
*  Some rights reserved.
*
*  Contact: http://www.wis.ewi.tudelft.nl/
*
**********************************************************/
package nl.wisdelft.twitter.analytics.redundancy.analyze;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * POJO for storing the Weka results.
 * 
 * @author Fabian Abel, <a href="mailto:f.abel@tudelft.nl">f.abel@tudelft.nl</a>
 * @version created on Feb 7, 2012
 */
public class WekaResults {

	public Double precision = 0.0;
	public Double recall = 0.0;
	public Double fmeasure = 0.0;
	
	public Map<String, Double> coefficients = new LinkedHashMap<String, Double>();
	public Double coefficientIntercept = 0.0;
}
