package gsn.msr.senseweb;
import gsn.beans.DataField;
import gsn.beans.VSensorConfig;
import gsn.utils.KeyValueImp;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import org.apache.commons.collections.KeyValue;

public class LoginToSenseWeb {

  public static void main(String[] args) throws RemoteException {
    String host= "http://micssrv22.epfl.ch/";
    VSensorConfig conf = new VSensorConfig();
    conf.setName("gsnTest1");
    conf.setDescription("Desc1");
    conf.setAddressing(new KeyValue[] {new KeyValueImp("latitude","1"),new KeyValueImp("Longitude","1"),new KeyValueImp("Altitude","1")});
    conf.setOutputStructure(new DataField[] {new DataField("tempetature","integer"),new DataField("light","double") });
    String username = "GSN@gsn.com";
    String password = "GSN@gsn.com";
    register_sensor(username, password, conf, host);
  }
  public static void register_sensor(String username,String password,VSensorConfig conf,String gsnURI ) throws RemoteException {

    gsn.msr.senseweb.usermanager.ServiceStub login = new gsn.msr.senseweb.usermanager.ServiceStub();
    gsn.msr.senseweb.usermanager.ServiceStub.GetPassCode getPassCodeParams = new gsn.msr.senseweb.usermanager.ServiceStub.GetPassCode();

    getPassCodeParams.setUserName(username);
    getPassCodeParams.setPassword(password);
    String passcodeStr = login.GetPassCode(getPassCodeParams).getGetPassCodeResult().getGuid();
    System.out.println(passcodeStr);

    gsn.msr.senseweb.sensormanager.ServiceStub stub = new gsn.msr.senseweb.sensormanager.ServiceStub(); //the default implementation should point to the right endpoint
    gsn.msr.senseweb.sensormanager.ServiceStub.Guid passGUID = new gsn.msr.senseweb.sensormanager.ServiceStub.Guid();
    passGUID.setGuid(passcodeStr);
    gsn.msr.senseweb.sensormanager.ServiceStub.CreateVectorSensorType createVSensorTypeParams = new gsn.msr.senseweb.sensormanager.ServiceStub.CreateVectorSensorType();
    createVSensorTypeParams.setPassCode(passGUID);
    createVSensorTypeParams.setPublisherName(username);
    createVSensorTypeParams.setName("GSNStreamElement-"+conf.getName());
    createVSensorTypeParams.setUri(gsnURI+"#"+conf.getName());


    gsn.msr.senseweb.sensormanager.ServiceStub.ArrayOfString arrayOfString = new gsn.msr.senseweb.sensormanager.ServiceStub.ArrayOfString ();
    ArrayList<String> fields=  new ArrayList<String>();
    for (DataField df : conf.getOutputStructure())
      fields.add("Generic");
    arrayOfString.setString(fields.toArray(new String[] {}));
    createVSensorTypeParams.setComponentTypes(arrayOfString);

    String call_output = stub.CreateVectorSensorType(createVSensorTypeParams).getCreateVectorSensorTypeResult();
    System.out.println(">>OUTPUT OF CREATION OF SENSOR TYPE: "+call_output);

    gsn.msr.senseweb.sensormanager.ServiceStub.RegisterVectorSensor registerVectorSensorParams = new gsn.msr.senseweb.sensormanager.ServiceStub.RegisterVectorSensor();

    gsn.msr.senseweb.sensormanager.ServiceStub.SensorInfo sensor = new gsn.msr.senseweb.sensormanager.ServiceStub.SensorInfo();
    sensor.setDataType("Vector");
    sensor.setPublisherName(username);
    sensor.setUrl(gsnURI);
    sensor.setAltitude(conf.getAltitude());
    sensor.setLatitude(conf.getLatitude());
    sensor.setLongitude(conf.getLongitude());
    sensor.setDescription(conf.getDescription());
    sensor.setSensorName(conf.getName());
    sensor.setSensorType("http://micssrv22.epfl.ch/");
    sensor.setEntryTime(new GregorianCalendar());
    sensor.setWebServiceUrl(gsnURI);
    registerVectorSensorParams.setPassCode(passGUID);
    registerVectorSensorParams.setPublisherName(username);
    registerVectorSensorParams.setSensor(sensor);


     call_output = stub.RegisterVectorSensor(registerVectorSensorParams).getRegisterVectorSensorResult();
    System.out.println(call_output);

//
//    out = sensors.insertVectorSensor(username, passcode, sensor );
//    System.out.println(">>OUTPUT OF INSERTION: "+out);
//    System.out.println("Registered Sensors ...");
//    List<SensorInfo> info = sensors.getSensorsByPublisher(username, passcode).getSensorInfo();
//    for (SensorInfo si : info) {
//      System.out.println(si.getSensorName());
//      sensors.deleteSensor(username, passcode, si.getSensorName());
//    }
  }

}
