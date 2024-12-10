/**
 * 
 * This class is part of the Programming the Internet of Things
 * project, and is available via the MIT License, which can be
 * found in the LICENSE file at the top level of this repository.
 * 
 * Copyright (c) 2020 by Andrew D. King
 */ 

package programmingtheiot.part02.integration.connection;

import static org.junit.Assert.*;

import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.SensorData;
import programmingtheiot.gda.connection.RedisPersistenceAdapter;

/**
 * This test case class contains very basic integration tests for
 * RedisPersistenceAdapter. It should not be considered complete,
 * but serve as a starting point for the student implementing
 * additional functionality within their Programming the IoT
 * environment.
 *
 */
public class PersistenceClientAdapterTest
{
	// static
	
	private static final Logger _Logger =
		Logger.getLogger(PersistenceClientAdapterTest.class.getName());

		public static final String DEFAULT_Actuator_NAME = "ActuatorDataFooBar";
		public static final String DEFAULT_Sensor_NAME = "SensorDataFooBar";
		public static final int DEFAULT_CMD = 1;
		public static final float DEFAULT_VAL = 10.0f;
	
	
	// member var's
	
	private RedisPersistenceAdapter rpa = null;
	
	
	// test setup methods
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception
	{
		this.rpa = new RedisPersistenceAdapter();
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception
	{
	}
	
	// test methods
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#connectClient()}.
	 */
	//@Test
	public void testConnectClient()
	{
		assertTrue(this.rpa.connectClient());
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#disconnectClient()}.
	 */
	//@Test
	public void testDisconnectClient()
	{
		int delay = 1;
		assertTrue(this.rpa.connectClient());
		
		try {
			Thread.sleep(delay * 1000 + 5000);
		} catch (Exception e) {
			// ignore
		}
		
		assertTrue(this.rpa.disconnectClient());
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#getActuatorData(java.lang.String, java.util.Date, java.util.Date)}.
	 */
	//@Test
	public void testGetActuatorData()
	{
		fail("Not yet implemented"); // TODO
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#getSensorData(java.lang.String, java.util.Date, java.util.Date)}.
	 */
	//@Test
	public void testGetSensorData()
	{
		fail("Not yet implemented"); // TODO
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#storeData(java.lang.String, int, programmingtheiot.data.ActuatorData[])}.
	 */
	//@Test
	public void testStoreDataStringIntActuatorDataArray()
	{
		int delay = 1;
		assertTrue(this.rpa.connectClient());

		try {
			Thread.sleep(delay * 1000 + 5000);
		} catch (Exception e) {
			// ignore
		}

		assertTrue(this.rpa.storeData(DEFAULT_Actuator_NAME, 0, createTestActuatorData()));

		try {
			Thread.sleep(delay * 1000 + 5000);
		} catch (Exception e) {
			// ignore
		}
		
		assertTrue(this.rpa.disconnectClient());

	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#storeData(java.lang.String, int, programmingtheiot.data.SensorData[])}.
	 */
	@Test
	public void testStoreDataStringIntSensorDataArray()
	{
		int delay = 1;
		assertTrue(this.rpa.connectClient());

		try {
			Thread.sleep(delay * 1000 + 5000);
		} catch (Exception e) {
			// ignore
		}

		assertTrue(this.rpa.storeData(DEFAULT_Sensor_NAME, 0, createTestSensorData()));

		try {
			Thread.sleep(delay * 1000 + 5000);
		} catch (Exception e) {
			// ignore
		}
		
		assertTrue(this.rpa.disconnectClient());
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#storeData(java.lang.String, int, programmingtheiot.data.SystemPerformanceData[])}.
	 */
	//@Test
	public void testStoreDataStringIntSystemPerformanceDataArray()
	{
		fail("Not yet implemented"); // TODO
	}

	private ActuatorData createTestActuatorData()
	{
		ActuatorData ad = new ActuatorData();
		ad.setName(DEFAULT_Actuator_NAME);
		ad.setCommand(DEFAULT_CMD);
		ad.setValue(DEFAULT_VAL);
		
		return ad;
	}

	private SensorData createTestSensorData()
	{
		SensorData sd = new SensorData();
		sd.setName(DEFAULT_Sensor_NAME);
		sd.setValue(DEFAULT_VAL);
		
		return sd;
	}

	
}
