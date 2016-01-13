package nl.anchormen.sql4es;

import java.sql.SQLException;

import nl.anchormen.sql4es.model.Heading;

public interface QueryState {

	/**
	 * Provides the original query provided to the driver
	 * @return
	 */
	public String originalSql();

	/**
	 * Returns the heading build for the query
	 * @return
	 */
	public Heading getHeading();
	
	/**
	 * Adds exception to this state signaling something went wrong while traversing the AST
	 * @param msg
	 */
	public void addException(String msg);
	
	/**
	 * 
	 * @return true if an exception has been set (typically a flag to stop work and return)
	 */
	public boolean hasException();
	
	/**
	 * @return the SQLExceptoin set in this state or NULL if it has no exception
	 */
	public SQLException getException();
	
	/**
	 * Gets the specified integer property
	 * @param name
	 * @param def
	 * @return
	 */
	public int getIntProp(String name, int def);
	
	/**
	 * Gets the specified string property
	 * @param name
	 * @param def
	 * @return
	 */
	public String getProperty(String name, String def);
	
}
