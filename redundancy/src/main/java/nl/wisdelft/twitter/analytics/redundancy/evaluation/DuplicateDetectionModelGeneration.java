/*********************************************************
*  Copyright (c) 2011 by Web Information Systems (WIS) Group.
*  Ke Tao, http://taubau.info/
*
*  Some rights reserved.
*
*  Contact: http://www.wis.ewi.tudelft.nl/
*
**********************************************************/
package nl.wisdelft.twitter.analytics.redundancy.evaluation;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

import weka.classifiers.meta.CostSensitiveClassifier;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

/**
 * Given an ARFF data file, the classification model can be outputed as a file.
 * 
 * The outputed file can be used for further use.
 * 
 * @author Ke Tao, <a href="mailto:k.tao@tudelft.nl">k.tao@tudelft.nl</a>
 * @author last edited by: ktao
 * 
 * @version created on May 23, 2013
 */
public class DuplicateDetectionModelGeneration {

	public static String costMatrix = 
			"0.0  3.5; " +
			"1.0  0.0";
	
	/**
	 * @param args
	 * args[0] : the url to the arff file
	 * args[1] : output model file
	 */
	public static void main(String[] args) {
		try {
			DataSource source = new DataSource(args[0]);
			Instances data = source.getDataSet();
			
			// setting class attribute if the data format does not provide this information
			// E.g., the XRFF format saves the class attribute information as well
			if (data.classIndex() == -1){
				data.setClassIndex(data.numAttributes() - 1);
			}
			
			// train classifier
			// example configuration:
			// weka.classifiers.meta.CostSensitiveClassifier
			// -cost-matrix "[0.0 3.5; 1.0 0.0]" -M -S 1 -W weka.classifiers.functions.Logistic -- -R 1.0E-8 -M -1
			CostSensitiveClassifier costSensitive = new CostSensitiveClassifier();
			costSensitive.setOptions(
					weka.core.Utils.splitOptions("-cost-matrix \"[" + costMatrix + "]\" -M -S 1 -W weka.classifiers.functions.Logistic -- -R 1.0E-8 -M -1"));
			
			costSensitive.buildClassifier(data);
			
			// Serialize model (Store into the file given by args[1])
			ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(args[1]));
			
			oos.writeObject(costSensitive);
			oos.flush();
			oos.close();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
