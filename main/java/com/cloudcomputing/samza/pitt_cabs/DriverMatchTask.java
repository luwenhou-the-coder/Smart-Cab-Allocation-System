package com.cloudcomputing.samza.pitt_cabs;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

    private KeyValueStore<String, Map<String, String>> blockDrivers;
    private KeyValueStore<String, Map<String, String>> driverInfo;
    private final double MAX_DIST = Math.sqrt((double) (500 * 500 * 2));
    private KeyValueStore<String, List<Double>> blockSPF;

    /* Define per task state here. (kv stores etc) */
    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        //Initialize stuff (maybe the kv stores?)
        blockDrivers = (KeyValueStore<String, Map<String, String>>) context.getStore("block-drivers");
        driverInfo = (KeyValueStore<String, Map<String, String>>) context.getStore("driver-info");
        blockSPF = (KeyValueStore<String, List<Double>>) context.getStore("block-SPF");
        //System.out.println("*********Initialization success!**********");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        // The main part of your code. Remember that all the messages for a particular partition
        // come here (somewhat like MapReduce). So for task 1 messages for a blockId will arrive
        // at one task only, thereby enabling you to do stateful stream processing.
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            processDriverLocation((Map<String, Object>) envelope.getMessage());
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            processEvent((Map<String, Object>) envelope.getMessage(), collector);
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        //this function is called at regular intervals, not required for this project
    }

    public void processDriverLocation(Map<String, Object> driverLoc) {
        //this method receive the stream of 'driver-ocations' and process it
        if (!driverLoc.get("type").equals("DRIVER_LOCATION")) {
            //double-check the type of the stream, throw an exception if it does not match
            throw new IllegalStateException("Unexpected event type on follows stream: " + driverLoc.get("type"));
        }
        String blockId = ((Integer) driverLoc.get("blockId")).toString();
        String driverId = ((Integer) driverLoc.get("driverId")).toString();
        Integer latitude = (Integer) driverLoc.get("latitude");
        Integer longitude = (Integer) driverLoc.get("longitude");

        if (blockDrivers.get(blockId) == null) {
            //if the block does not have any drivers yet, create a new map
            Map<String, String> driverLocation = new HashMap<String, String>();
            driverLocation.put(driverId, latitude.toString() + "," + longitude.toString());
            blockDrivers.put(blockId, driverLocation);

        } else {
            //if it already had drivers, simply update the map
            blockDrivers.get(blockId).put(driverId, latitude.toString() + "," + longitude.toString());
        }
        //System.out.println("-----------Successfully added one driver to block " + blockId + " -----------");
    }

    public void processEvent(Map<String, Object> event, MessageCollector collector) {
        //this method receive the stream of 'events', process it and finally output a 'match' stream
        String type = (String) event.get("type");
        String blockId = ((Integer) event.get("blockId")).toString();
        if (type.equals("LEAVING_BLOCK")) {
            //if it is a 'leaving block' event
            if (blockDrivers.get(blockId) != null) {
                String driverId = ((Integer) event.get("driverId")).toString();
                blockDrivers.get(blockId).remove(driverId);  //remove the driver from the block    
                // System.out.println("##########Driver <" + driverId + "> left block <" + blockId + ">#########");

            }
        } else if (type.equals("ENTERING_BLOCK")) {
            //if it is an 'entering block' event
            String driverId = ((Integer) event.get("driverId")).toString();
            Integer latitude = (Integer) event.get("latitude");
            Integer longitude = (Integer) event.get("longitude");
            String status = (String) event.get("status");

            if (status.equals("AVAILABLE")) {
                //if the driver is available, add him to the block and update his personal information. Otherwise do nothing
                if (blockDrivers.get(blockId) != null) {
                    //if the block has drivers, update
                    blockDrivers.get(blockId).put(driverId, latitude.toString() + "," + longitude.toString());
                } else {
                    //if the block does not have any driver, create a new map 
                    Map<String, String> driverLocation = new HashMap<String, String>();
                    driverLocation.put(driverId, latitude.toString() + "," + longitude.toString());
                    blockDrivers.put(blockId, driverLocation);
                }
            }
            String gender = (String) event.get("gender");
            Double rating = (Double) event.get("rating");
            Integer salary = (Integer) event.get("salary");
            Map<String, String> driverDetail = new HashMap<String, String>();
            driverDetail.put("gender", gender);
            driverDetail.put("rating", rating.toString());
            driverDetail.put("salary", salary.toString());
            driverInfo.put(driverId, driverDetail);
            //System.out.println("##########Driver <" + driverId + "> entered and is availabe in block <" + blockId + ">#########");

        } else if (type.equals("RIDE_REQUEST")) {
            //if it is a customer's request for a car
            outputMatch(event, collector);
        } else if (type.equals("RIDE_COMPLETE")) {
            // if it is an event that indicates the ride is complete
            String driverId = ((Integer) event.get("driverId")).toString();
            Integer latitude = (Integer) event.get("latitude");
            Integer longitude = (Integer) event.get("longitude");

            if (blockDrivers.get(blockId) != null) {
                //if the block has drivers, update
                blockDrivers.get(blockId).put(driverId, latitude.toString() + "," + longitude.toString());
            } else {
                //if the block does not have any driver, create a new map 
                Map<String, String> driverLocation = new HashMap<String, String>();
                driverLocation.put(driverId, latitude.toString() + "," + longitude.toString());
                blockDrivers.put(blockId, driverLocation);
            }
            String gender = (String) event.get("gender");
            Double rating = (Double) event.get("rating");
            Integer salary = (Integer) event.get("salary");
            Map<String, String> driverDetail = new HashMap<String, String>();
            driverDetail.put("gender", gender);
            driverDetail.put("rating", rating.toString());
            driverDetail.put("salary", salary.toString());
            driverInfo.put(driverId, driverDetail);
            //System.out.println("##########Driver <" + driverId + "> completed in block <" + blockId + ">#########");
        }
    }

    private void outputMatch(Map<String, Object> event, MessageCollector collector) {
        // Colon is used as separator, and semicolon is lexicographically after colon
        String blockId = ((Integer) event.get("blockId")).toString();
        if (blockDrivers.get(blockId) != null) {
            String clientId = ((Integer) event.get("clientId")).toString();
            Integer clientLatitude = (Integer) event.get("latitude");
            Integer clientLongitude = (Integer) event.get("longitude");
            String genderPrefer = (String) event.get("gender_preference");

            Map<String, String> allDrivers = blockDrivers.get(blockId);
            double maxScore = 0;
            String maxDriver = "";
            double driverRatio = 0;

            for (Entry<String, String> oneDriver : allDrivers.entrySet()) {
                //calcute the score for each available driver and put it into a map for later comparison
                String driverId = oneDriver.getKey();
                String driverLocation = oneDriver.getValue();
                String[] parts = driverLocation.split(",");
                Integer driverLatitude = Integer.parseInt(parts[0]);
                Integer driverLongitude = Integer.parseInt(parts[1]);
                double distance = Math.sqrt(Math.pow((double) (driverLatitude - clientLatitude), 2.0)
                        + Math.pow((double) (driverLongitude - clientLongitude), 2.0));
                double distScore = 1 - distance / MAX_DIST;

                Map<String, String> driverDetail = driverInfo.get(driverId);
                if (driverDetail != null) {
                    String gender = driverDetail.get("gender");
                    Double rating = Double.parseDouble(driverDetail.get("rating"));
                    Double salary = Double.parseDouble(driverDetail.get("salary"));

                    double genderScore = 0;
                    if (genderPrefer.equals("N") || genderPrefer.equals(gender)) {
                        genderScore = 1.0;
                    }
                    double ratingScore = rating / 5.0;
                    double salaryScore = 1 - salary / 100.0;
                    double matchScore = distScore * 0.4 + genderScore * 0.2 + ratingScore * 0.2 + salaryScore * 0.2;
                    if (matchScore > maxScore) {
                        //find the match driver with the highest score
                        maxScore = matchScore;
                        maxDriver = driverId;
                    }
                    driverRatio++;
                }
            }

            //calculate the SPF
            List<Double> currentList = (List) blockSPF.get(blockId);
            double SPF = 1.0;   //default SPF value
            if (currentList == null) {
                //if there is no list for the block, create one
                List<Double> driverRatioList = new LinkedList<Double>();
                driverRatioList.add(driverRatio);
                blockSPF.put(blockId, driverRatioList);
            } else {
                //if there is a list for the block
                if (currentList.size() < 5) {
                    //if the size of the list is smaller than 5
                    if (currentList.size() == 4) {
                        //if this is the 5th request, calculate SPF
                        double sum = 0;
                        for (int i = 0; i < 4; i++) {
                            sum += currentList.get(i);
                        }
                        sum += driverRatio;
                        double average = sum / 5;
                        System.out.println("############average is: " + average);
                        if (average < 3.6) {
                            double SF = (4 * (3.6 - average) / (1.8 - 1));
                            SPF = 1 + SF;
                            System.out.println("****************SPF changes to: " + SPF);
                        }
                    }
                    blockSPF.get(blockId).add(driverRatio);
                } else {
                    //if the size of the list is exactly 5
                    currentList.remove(0);  //keep the maximum size of the list always at 5
                    double sum = 0;
                    for (int i = 0; i < 4; i++) {
                        sum += currentList.get(i);
                    }
                    sum += driverRatio;
                    double average = sum / 5;
                    System.out.println("############average is: " + average);
                    if (average < 3.6) {
                        double SF = (4 * (3.6 - average) / (1.8 - 1));
                        SPF = 1 + SF;
                        System.out.println("****************SPF changes to: " + SPF);
                    }
                    blockSPF.get(blockId).remove(0);    //keep the maximum size of the list always at 5
                    blockSPF.get(blockId).add(driverRatio);
                }
            }

            blockDrivers.get(blockId).remove(maxDriver);  //remove the driver from the block      
            //System.out.println("-----------Successfully called driver <" + maxDriver + "> for client <" + clientId + "> in block <" + blockId + "> -----------");

            Map<String, Object> output = new HashMap<String, Object>();
            output.put("clientId", clientId);
            output.put("driverId", maxDriver);
            output.put("priceFactor", SPF);
            collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, output));    //output the client with the matching driver

        }
    }
}
