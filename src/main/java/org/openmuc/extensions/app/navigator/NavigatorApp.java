package org.openmuc.extensions.app.navigator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Dictionary;
import java.util.Map;
import java.util.Timer;

import org.openmuc.framework.dataaccess.DataAccessService;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.siemens.eadvantage.ie.ImportEngineClient;

/**
 * Transfers logged data from OpenMUC to Siemens Advantage Navigator.
 * 
 * @author Mike Pichler
 *
 */
public class NavigatorApp implements ManagedService {
	
	private final static Logger logger = LoggerFactory.getLogger(NavigatorApp.class);
	private final static long twentyfour_hours_in_mills = 1000 * 60 * 60 * 24;
	private final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private Timer timer = null;
	
	// Properties
	protected String username, password;
	protected long firstUploadDate = 0;
	protected long uploadInterval = twentyfour_hours_in_mills;
	protected long dataRange = twentyfour_hours_in_mills;
	protected String importEngineUrl = ImportEngineClient.DEFAULT_IMPORT_ENGINE_URL;
	protected String reportErrorChannelId = null;
	protected boolean deleteFileAfterUpload = true;
	
	private DataAccessService dataAccessService = null;
	
	protected void setDataAccessService(DataAccessService service) {
		dataAccessService = service;
	}
	
	protected void unsetDataAccessService(DataAccessService service) {
		dataAccessService = null;
	}
	
	protected void activate(ComponentContext context, Map<String, Object> properties) {	
		try {
			firstUploadDate = dateParser.parse("2015-01-01 00:00:00").getTime();
		} catch (ParseException cannotHappen) {
			throw new AssertionError();
		}
		logger.info("Advantage Navigator app activated, waiting for configuration properties");
	}
		
	protected void deactivate(ComponentContext context) {
		if(timer != null) timer.cancel();
		timer = null;
		logger.info("Advantage Navigator app deactivated");
	}
	
	@Override
	public void updated(@SuppressWarnings("rawtypes") Dictionary properties) throws ConfigurationException {
		
		if(properties == null) { // no configuration available for this component
			logger.error("No configuration available for this component, cannot perform uploads");
		} else {
			
			// parse properties
			username = getUsername(properties);
			password = getPassword(properties);
			
			try {
				if(properties.get("dataRange") != null) dataRange = Long.parseLong((String) properties.get("dataRange"));
				if(properties.get("deleteFileAfterUpload") != null) deleteFileAfterUpload = Boolean.parseBoolean((String) properties.get("deleteFileAfterUpload"));
				if(properties.get("importEngineUrl") != null) importEngineUrl = (String) properties.get("importEngineUrl");
				if(properties.get("firstUploadDate") != null) firstUploadDate = dateParser.parse((String) properties.get("firstUploadDate")).getTime();
				if(properties.get("uploadInterval") != null) uploadInterval = Long.parseLong((String) properties.get("uploadInterval"));
				reportErrorChannelId = (String) properties.get("reportErrorChannelId");
			} catch (NumberFormatException e) {
				throw new ConfigurationException("component properties", e.getMessage());
			} catch (ParseException e) {
				throw new ConfigurationException("component properties", e.getMessage());
			}
			
			// start upload task
			startUploadTimer();
			
		}		
	}
	
	private void startUploadTimer() {	
		
		ImportEngineClient importEngineClient = new ImportEngineClient(username, password);
		importEngineClient.setImportEngineUrl(importEngineUrl);
		importEngineClient.setDeleteCommandFileAfterUpload(deleteFileAfterUpload);
		
		UploadTask uploadTask = new UploadTask(dataAccessService, importEngineClient);
		uploadTask.setDataRange(dataRange);
		if(reportErrorChannelId != null) uploadTask.setErrorChannel(dataAccessService.getChannel(reportErrorChannelId));
		
		if(timer != null) timer.cancel();
		timer = new Timer("NavigatorApp upload task", true);
		
		long nextRun = getNextRun(firstUploadDate, uploadInterval);
		timer.scheduleAtFixedRate(uploadTask, nextRun, uploadInterval);
		
		logger.debug("started upload task with interval of {}ms, next upload will be at {}", uploadInterval, new Date(nextRun));
	}

	private String getUsername(@SuppressWarnings("rawtypes") Dictionary properties) throws ConfigurationException {
		String username = (String) properties.get("username");
		if(username==null) throw new ConfigurationException("username", "is missing");
		if(username.isEmpty()) throw new ConfigurationException("username", "is empty");
		return username;
	}
	
	private String getPassword(@SuppressWarnings("rawtypes") Dictionary properties) throws ConfigurationException {
		String password = (String) properties.get("password");
		if(password==null) throw new ConfigurationException("password", "is missing");
		if(password.isEmpty()) throw new ConfigurationException("password", "is empty");
		return password;
	}
	
	private long getNextRun(long firstTime, long interval) {
		// calculate the next time stamp to run 
		long now = System.currentTimeMillis();	
		return now - ((now - firstTime) % interval) + interval;
	}
}
