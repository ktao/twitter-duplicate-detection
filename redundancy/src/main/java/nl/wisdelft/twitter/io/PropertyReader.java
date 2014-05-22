/***************************************************************
*  Copyright (c) 2010 by GRAPPLE Project (http://www.grapple-project.org)
*  Some rights reserved.
*
*  This file is part of the GRAPPLE Project. 
*  
*  Contact: http://www.grapple-project.org
*
*  This copyright notice MUST APPEAR in all copies of the file!
***************************************************************/
package nl.wisdelft.twitter.io;

import java.util.MissingResourceException;
import java.util.ResourceBundle;


/**
 * This class reads in the mypes.properties file.
 * 
 * @author Fabian Abel, <a href="mailto:abel@l3s.de">abel@l3s.de</a>
 * @author last edited by: $Author: fabian $
 * 
 * @version created on May 7, 2009
 * @version $Revision: 1.1 $ $Date: 2011-01-25 18:46:40 $
 */
public class PropertyReader {
	private static final String BUNDLE_NAME = "org.wis.twinder.io.twitterdb"; //$NON-NLS-1$

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle
			.getBundle(BUNDLE_NAME);

	private PropertyReader() {
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
}
