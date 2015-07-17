package org.openmuc.extensions.app.navigator;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.openmuc.framework.data.BooleanValue;
import org.openmuc.framework.data.Record;
import org.openmuc.framework.dataaccess.Channel;
import org.openmuc.framework.dataaccess.DataAccessService;
import org.openmuc.framework.dataaccess.DataLoggerNotAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.siemens.eadvantage.ie.ImportEngineClient;
import com.siemens.eadvantage.ie.command.MeterReadingCommand;
import com.siemens.eadvantage.ie.types.DateTime;
import com.siemens.eadvantage.ie.types.MeterReading;

/**
 * This timer task implementation sends logged data records to the Advantage Navigator import engine.<p>
 * 
 * The default data range is 24 hours (can be changed using the <code>setDataRange()</code> method). The
 * maximum number of records for a single transmission is 5000. If more than 5000 records are available in
 * the specified data range, several transmissions will be performed automatically.
 * 
 * @author Mike Pichler
 *
 */
public class UploadTask extends TimerTask {

	private final static Logger logger = LoggerFactory.getLogger(UploadTask.class);
	private final DataAccessService dataAccessService;
	private final ImportEngineClient importEngineClient;
	
	private long dataRange = 1000 * 60 * 60 * 24; // 24 hours
	private Channel errorChannel = null;
	
	public UploadTask(final DataAccessService service, final ImportEngineClient client) {
		
		if(service == null) throw new IllegalArgumentException("DataAccessService must not be null");
		if(client == null) throw new IllegalArgumentException("ImportEngineClient must not be null");
		
		this.dataAccessService = service;
		this.importEngineClient = client;
	}
	
	@Override
	public void run() {
		
		// get channels from OpenMUC data manager
		List<String> channelIds = dataAccessService.getAllIds();
		
		MeterReadingCommand command = new MeterReadingCommand();
		
		final long startTime = System.currentTimeMillis();
		int recordCount = 0;
		
		for (String channelId : channelIds) {
			Channel channel = dataAccessService.getChannel(channelId);
			try {
				List<Record> records = channel.getLoggedRecords(startTime - dataRange);
				for (Record record : records) {
					command.addMeterReading(new MeterReading(channelId, new DateTime(new Date(record.getTimestamp())), record.getValue().asDouble()));
					recordCount++;
					
					if(recordCount % 5000 == 0) {	// max. number of data records in a single transmission is 5000
						String result = importEngineClient.executeCommand(command);
						logger.debug("Sent 5000 records to import engine ({})", result);
						command = new MeterReadingCommand();
					}
				}
					
			} catch (DataLoggerNotAvailableException e) {
				logger.error("Could not retrive logged data because data logger is not available, upload task stopped");
				setErrorChannel(true);
				return;
			} catch (IOException e) {
				logger.error("Error while retrieving logged data: " + e.getMessage());
			}		
		}
		
		try {
			
			if(recordCount == 0) {
				logger.warn("No records found to send between {} and {}", new Date(startTime-dataRange).toString(), new Date(startTime).toString());
				return;
			}
				
			String result = importEngineClient.executeCommand(command);
			long executionTime = System.currentTimeMillis() - startTime;
			
			setErrorChannel(false);
			logger.info("Transfer of {} data record(s) to import engine successfully finished in {}ms ({})", recordCount, executionTime, result);
			
		} catch (IOException e) {
			logger.error("Error while sending meter readings to import engine: " + e.getMessage());
			setErrorChannel(true);
		}		
		
	}

	public long getDataRange() {
		return dataRange;
	}

	public void setDataRange(long dataRange) {
		this.dataRange = dataRange;
	}
	
	public Channel getErrorChannel() {
		return errorChannel;
	}

	public void setErrorChannel(Channel errorChannel) {
		this.errorChannel = errorChannel;
	}

	private void setErrorChannel(boolean error) {
		if(errorChannel != null) {
			errorChannel.write(new BooleanValue(error));
		}
	}
	
}
