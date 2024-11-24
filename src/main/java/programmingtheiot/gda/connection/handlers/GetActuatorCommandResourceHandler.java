package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.server.resources.CoapExchange;

import programmingtheiot.common.IActuatorDataListener;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;

public class GetActuatorCommandResourceHandler extends
 CoapResource implements IActuatorDataListener {
    
    private static final Logger _Logger = 
        Logger.getLogger(GetActuatorCommandResourceHandler.class.getName());

    private ActuatorData actuatorData = null;

    //
    public GetActuatorCommandResourceHandler(String resourceName){
        super(resourceName);

        //set resource to be observable
        super.setObservable(true);
    }

    public boolean onActuatorDataUpdate(ActuatorData data){
        
        if(data != null && this.actuatorData != null){
            this.actuatorData.updateData(data);

            // notify all connected clients
            super.changed();

            _Logger.fine("Actuator data updated for URI: " + super.getURI() + 
            ": Data value = " + this.actuatorData.getValue());

            return true;
        }

        return false;
    }

    @Override
    public void handleGET(CoapExchange context){
        //accept the request
        context.accept();

        //locally stored actuator data to json data
        String jsonData = DataUtil.getInstance().actuatorDataToJson(this.actuatorData);

        //send response
        context.respond(ResponseCode.CONTENT, jsonData);
    }



}
