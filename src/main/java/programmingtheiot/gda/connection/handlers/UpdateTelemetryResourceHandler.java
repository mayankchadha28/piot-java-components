package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;

import programmingtheiot.common.IDataMessageListener;

public class UpdateTelemetryResourceHandler extends CoapResource {
    private IDataMessageListener dataMsgListener = null;

    private static final Logger _Logger =
		Logger.getLogger(UpdateSystemPerformanceResourceHandler.class.getName());

    public UpdateTelemetryResourceHandler(String resourceName){
        super(resourceName);
    }

    public void setDataMessageListener(IDataMessageListener listener){
        if(listener != null){
            this.dataMsgListener = listener;
        }
    }

}
