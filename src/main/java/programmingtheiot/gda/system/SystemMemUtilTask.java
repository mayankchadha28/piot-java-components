/**
 * This class is part of the Programming the Internet of Things project.
 * 
 * It is provided as a simple shell to guide the student and assist with
 * implementation for the Programming the Internet of Things exercises,
 * and designed to be modified by the student as needed.
 */ 

package programmingtheiot.gda.system;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

import java.util.logging.Logger;

import programmingtheiot.common.ConfigConst;

/**
 * Class that extends from the abstract class BaseSystemUtilTask 
 * to get telemetry values for System Memory Utilization.
 * 
 */
public class SystemMemUtilTask extends BaseSystemUtilTask
{
	// constructors
	private static final Logger _Logger = Logger.getLogger(SystemPerformanceManager.class.getName());
	/**
	 * Default.
	 * 
	 */
	public SystemMemUtilTask()
	{
		super(ConfigConst.NOT_SET, ConfigConst.DEFAULT_TYPE_ID);
	}
	
	
	// public methods
	
	@Override
	public float getTelemetryValue()
	{
		//Calling methods for memory used and max memory
		MemoryUsage memUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
		double memUsed = (double) memUsage.getUsed();
		double memMax = (double) memUsage.getMax();

		_Logger.fine("Mem used: "+ memUsed + "; Mem Max: " + memMax);

		double memUtil = (memUsed / memMax) * 100.0d;

		return (float) memUtil;

	}
	
}
