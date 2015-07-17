package org.openmuc.extensions.app.navigator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.openmuc.framework.data.DoubleValue;
import org.openmuc.framework.data.Flag;
import org.openmuc.framework.data.IntValue;
import org.openmuc.framework.data.Record;
import org.openmuc.framework.dataaccess.Channel;
import org.openmuc.framework.dataaccess.DataAccessService;

import com.siemens.eadvantage.ie.ImportEngineClient;
import com.siemens.eadvantage.ie.command.Command;
import com.siemens.eadvantage.ie.command.MeterReadingCommand;

public class TestUploadTask {
	
	@Test
	public void testNoRecords() throws IOException {
		
		DataAccessService dataAccessService = mock(DataAccessService.class);
		ImportEngineClient importEngineClient = mock(ImportEngineClient.class);
		
		UploadTask task = new UploadTask(dataAccessService, importEngineClient);
		task.run();
		
		verify(importEngineClient, times(0)).executeCommand((Command) anyObject());
	}
	
	@Test
	public void testUpload() throws Throwable {
		
		DataAccessService dataAccessService = createSampleData(3000);
		ImportEngineClient importEngineClient = mock(ImportEngineClient.class);
		
		UploadTask task = new UploadTask(dataAccessService, importEngineClient);
		task.run();
		
		verify(importEngineClient, times(1)).executeCommand((Command) anyObject());
		
	}
	
	@Test
	public void testSegmentation() throws Throwable {
		
		DataAccessService dataAccessService = createSampleData(5001);
		ImportEngineClient importEngineClient = mock(ImportEngineClient.class);
		
		UploadTask task = new UploadTask(dataAccessService, importEngineClient);
		task.run();
		
		verify(importEngineClient, times(2)).executeCommand((Command) anyObject());
		
	}
	
	@Test
	public void testSendLatestRecord() throws Throwable {
		
		DataAccessService dataAccessService = mock(DataAccessService.class);
		Record record = new Record(new DoubleValue(10.0d), 10000l, Flag.VALID);
		Channel channel = mock(Channel.class);
		when(channel.getLatestRecord()).thenReturn(record);
		when(dataAccessService.getChannel(anyString())).thenReturn(channel);
		
		List<String> channelIds = Arrays.asList("channel1");
		when(dataAccessService.getAllIds()).thenReturn(channelIds);
		
		ImportEngineClient importEngineClient = mock(ImportEngineClient.class);
		
		
			
		UploadTask task = new UploadTask(dataAccessService, importEngineClient);
		task.setDataRange(0);
		task.run();
		
		verify(importEngineClient, times(1)).executeCommand((Command) anyObject());
		
		// check argument passed to mock
		ArgumentCaptor<MeterReadingCommand> argument = ArgumentCaptor.forClass(MeterReadingCommand.class);
		verify(importEngineClient).executeCommand(argument.capture());		
		assertThat(argument.getValue().getReadings().size(), is(1));
		assertThat(argument.getValue().getReadings().get(0).getValue(), is(10.0d));
		
	}
	
	private DataAccessService createSampleData(int numberOfRecords) throws Throwable {
		
		DataAccessService dataAccessService = mock(DataAccessService.class);
		
		List<String> channelIds = Arrays.asList("channel1", "channel2", "channel3");
		when(dataAccessService.getAllIds()).thenReturn(channelIds);
		
		Channel channel1 = mock(Channel.class);
		when(dataAccessService.getChannel("channel1")).thenReturn(channel1);
		
		Channel channel2 = mock(Channel.class);
		when(dataAccessService.getChannel("channel2")).thenReturn(channel2);
		
		Channel channel3 = mock(Channel.class);
		when(dataAccessService.getChannel("channel3")).thenReturn(channel3);
		
		List<Record> records = new ArrayList<Record>();
		final long startTime = System.currentTimeMillis() - numberOfRecords * 1000 * 60 * 15;
		for(int index = 0; index < numberOfRecords; index++) {
			Record record = new Record(new IntValue(index), startTime + index * 1000 * 60 * 15);
			records.add(record);
		}
		when(channel1.getLoggedRecords(anyLong())).thenReturn(records);
		
		return dataAccessService;
	}
	
}
