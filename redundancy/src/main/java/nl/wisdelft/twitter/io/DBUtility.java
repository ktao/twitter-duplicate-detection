package nl.wisdelft.twitter.io;
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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import javax.swing.Spring;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.object.BatchSqlUpdate;

/**
 * Some utility functionality to query MySQL DBs...
 * 
 * @author <a href="mailto:abel@l3s.de">Fabian Abel</a>
 * @version created on May 25, 2010
 */
public class DBUtility {

	/**
	 * the database where the Mypes-specific stuff is stored (e.g. the SDB RDF
	 * store)
	 */
	public static DriverManagerDataSource ds = new DriverManagerDataSource();
	

	static {
		ds.setDriverClassName(PropertyReader.getString("db.driver")); //$NON-NLS-1$
		ds.setUrl(PropertyReader.getString("db.location")); //$NON-NLS-1$
		ds.setUsername(PropertyReader.getString("db.username")); //$NON-NLS-1$
		ds.setPassword(PropertyReader.getString("db.password")); //$NON-NLS-1$
	}

	/** single connection */
	public static Connection conn = null;
	
	/**
	 * Executes the given query on the standard data source
	 * 
	 * @param query
	 *            query to execute
	 * @return results of the query or <code>null</code> if an error occurred
	 */
	public static final ResultSet executeQuery(String query) {

		return executeQuery(query, ds);
	}

	public static final ResultSet executeQuerySingleConnection(String query) {

		return executeQuerySingleConnection(query, ds);
	}

	/**
	 * Executes the given query on the given data source
	 * 
	 * @param query
	 *            query to execute
	 * @param datasource
	 *            the data source where the query should be executed
	 * @return results of the query or <code>null</code> if an error occurred
	 */
	public static final ResultSet executeQuery(String query,
			DriverManagerDataSource datasource) {
		Statement sqlStatement = null;
		try {
			sqlStatement = datasource.getConnection().createStatement();
			ResultSet res = sqlStatement.executeQuery(query);
			return res;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Executes the given query on the given data source
	 * 
	 * @param query
	 *            query to execute
	 * @param datasource
	 *            the data source where the query should be executed
	 * @return results of the query or <code>null</code> if an error occurred
	 */
	public static final ResultSet executeQuerySingleConnection(String query,
			DriverManagerDataSource datasource) {
		Statement sqlStatement = null;
		try {
			if (conn == null || conn.isClosed()) {
				conn = datasource.getConnection();
			}
			sqlStatement = conn.createStatement();
			ResultSet res = sqlStatement.executeQuery(query);
			// datasource.getConnection().close();
			// sqlStatement.close();
			// conn.close();
			return res;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Executes the given query on the standard data source
	 * 
	 * @param query
	 *            query to execute
	 * @return results of the query or <code>null</code> if an error occurred
	 */
	public static final ResultSet executePrepareQuerySingleConnection(
			String query) {

		return executePrepareQuerySingleConnection(query, ds);
	}

	/**
	 * Executes the given query on the given data source
	 * 
	 * @param query
	 *            query to execute
	 * @param datasource
	 *            the data source where the query should be executed
	 * @return results of the query or <code>null</code> if an error occurred
	 */
	public static final ResultSet executePrepareQuerySingleConnection(
			String query, DriverManagerDataSource datasource) {
		Statement sqlStatement = null;
		try {
			if (conn == null) {
				conn = datasource.getConnection();
			}
			sqlStatement = conn.prepareStatement(query);
			ResultSet res = sqlStatement.executeQuery(query);
			// datasource.getConnection().close();
			// sqlStatement.close();
			// conn.close();
			return res;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Executes the given query on the standard data source
	 * 
	 * @param query
	 *            query to execute
	 * @return results of the query or <code>null</code> if an error occurred
	 */
	public static final int executeUpdateQuerySingleConnection(String query) {

		return executeUpdateQuerySingleConnection(query, ds);
	}

	/**
	 * Executes the given query on the given data source
	 * 
	 * @param query
	 *            query to execute
	 * @param datasource
	 *            the data source where the query should be executed
	 * @return results of the query or <code>null</code> if an error occurred
	 */
	public static final int executeUpdateQuerySingleConnection(
			String query, DriverManagerDataSource datasource) {
		Statement sqlStatement = null;
		try {
			if (conn == null) {
				conn = datasource.getConnection();
			}
			sqlStatement = conn.prepareStatement(query);
			int res = sqlStatement.executeUpdate(query);
			// datasource.getConnection().close();
			// sqlStatement.close();
			// conn.close();
			return res;
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public static void main(String args[]) {
	}
}
