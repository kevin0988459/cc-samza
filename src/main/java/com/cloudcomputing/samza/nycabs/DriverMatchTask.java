package com.cloudcomputing.samza.nycabs;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import com.google.gson.Gson;
import java.util.Map;
import java.util.HashMap;

import org.apache.samza.storage.kv.KeyValueIterator;

import static com.cloudcomputing.samza.nycabs.DriverMatchConfig.MATCH_STREAM;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {

    /* Define per task state here. (kv stores etc)
       READ Samza API part in Primer to understand how to start
     */
    private double MAX_MONEY = 100.0;
    private KeyValueStore<String, Map<String, Object>> driverStore;
    // private ObjectMapper objectMapper;
    private Gson gson;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe the kv stores?)
        driverStore = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("driver-loc");
        // objectMapper = new ObjectMapper();
        gson = new Gson();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
         */
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        String messageJson = envelope.getMessage().toString();
        try {
            JsonObject event = JsonParser.parseString(messageJson).getAsJsonObject();
            if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
                // Handle Driver Location messages
                handleDriverLocation(event);
            } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
                // Handle Event messages
                handleEvent(event, collector);
            } else {
                throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle driver location messages
     *
     * @param jsonNode
     */
    private void handleDriverLocation(JsonObject locationEvent) {
        int blockId = locationEvent.get("blockId").getAsInt();
        int driverId = locationEvent.get("driverId").getAsInt();
        double latitude = locationEvent.get("latitude").getAsDouble();
        double longitude = locationEvent.get("longitude").getAsDouble();

        String key = constructKey(blockId, driverId);

        if (keyExists(key)) {
            Map<String, Object> driverInfo = driverStore.get(key);
            // Update driver location
            driverInfo.put("latitude", latitude);
            driverInfo.put("longitude", longitude);
            // put into kv store
            driverStore.put(key, driverInfo);
        }
        // ignore if the key does not exist
    }

    /**
     * Handle events message
     *
     * @param jsonNode
     * @param collector
     */
    private void handleEvent(JsonObject event, MessageCollector collector) {
        String eventType = event.get("type").getAsString();
        switch (eventType) {
            case "ENTERING_BLOCK":
                handleEnteringBlock(event);
                break;
            case "LEAVING_BLOCK":
                handleLeavingBlock(event);
                break;
            case "RIDE_REQUEST":
                handleRideRequest(event, collector);
                break;
            case "RIDE_COMPLETE":
                handleRideComplete(event);
                break;
            default:
                System.err.println("Unknown event type: " + eventType);
        }
    }

    /**
     * Process ENTERING_BLOCK type to add driver to KV store as available
     * register available drivers into kv store
     *
     * @param event
     */
    private void handleEnteringBlock(JsonObject event) {
        int blockId = event.get("blockId").getAsInt();
        int driverId = event.get("driverId").getAsInt();
        double latitude = event.get("latitude").getAsDouble();
        double longitude = event.get("longitude").getAsDouble();
        String gender = event.get("gender").getAsString();
        double rating = event.get("rating").getAsDouble();
        int salary = event.get("salary").getAsInt();
        String status = event.get("status").getAsString();
        // only put into kv store if the driver is available
        if (status.equals("AVAILABLE")) {
            String key = constructKey(blockId, driverId);
            Map<String, Object> driverInfo = new HashMap<>();
            // driver info map
            driverInfo.put("driverId", driverId);
            driverInfo.put("blockId", blockId);
            driverInfo.put("latitude", latitude);
            driverInfo.put("longitude", longitude);
            driverInfo.put("status", status);
            driverInfo.put("gender", gender);
            driverInfo.put("rating", rating);
            driverInfo.put("salary", salary);
            // put into kv store
            driverStore.put(key, driverInfo);
        }
    }

    /**
     * Handle LEAVING_BLOCK type to remove driver from KV store
     *
     * @param event
     */
    private void handleLeavingBlock(JsonObject event) {
        int blockId = event.get("blockId").getAsInt();
        int driverId = event.get("driverId").getAsInt();

        String key = constructKey(blockId, driverId);
        // delete from kv store
        driverStore.delete(key);
    }

    /**
     * Handle RIDE_REQUEST type to find the best driver for the rider
     *
     * @param event
     * @param collector
     */
    private void handleRideRequest(JsonObject event, MessageCollector collector) {
        int blockId = event.get("blockId").getAsInt();
        int clientId = event.get("clientId").getAsInt();
        double clientLatitude = event.get("latitude").getAsDouble();
        double clientLongitude = event.get("longitude").getAsDouble();
        String genderPreference = event.get("gender_preference").getAsString();

        double highestMatchScore = -1;
        String bestDriverKey = null;
        String startKey = blockId + ":";
        String endKey = blockId + ":~";
        // iterate based on blockId
        KeyValueIterator<String, Map<String, Object>> driversInBlock = driverStore.range(startKey, endKey);

        try {
            while (driversInBlock.hasNext()) {
                org.apache.samza.storage.kv.Entry<String, Map<String, Object>> entry = driversInBlock.next();
                Map<String, Object> driverInfo = entry.getValue();
                if ("AVAILABLE".equals(driverInfo.get("status"))) {
                    double driverLat = (Double) driverInfo.get("latitude");
                    double driverLon = (Double) driverInfo.get("longitude");
                    String driverGender = (String) driverInfo.get("gender");
                    double driverRating = (Double) driverInfo.get("rating");
                    int driverSalary = (Integer) driverInfo.get("salary");

                    double distanceScore = computeDistanceScore(clientLatitude, clientLongitude, driverLat, driverLon);
                    double genderScore = calculateGenderScore(driverGender, genderPreference);
                    double ratingScore = driverRating / 5.0;
                    double salaryScore = 1 - (driverSalary / 100.0);

                    // Weighted sum of scores
                    double matchScore = 0.4 * distanceScore + 0.1 * genderScore
                            + 0.3 * ratingScore + 0.2 * salaryScore;

                    if (matchScore > highestMatchScore) {
                        highestMatchScore = matchScore;
                        bestDriverKey = entry.getKey();
                    }
                }
            }
        } finally {
            driversInBlock.close(); // Ensure the iterator is closed to release resources
        }

        if (bestDriverKey != null) {
            // Mark the driver as UNAVAILABLE
            Map<String, Object> bestDriverInfo = driverStore.get(bestDriverKey);
            bestDriverInfo.put("status", "UNAVAILABLE");
            driverStore.put(bestDriverKey, bestDriverInfo);

            // Extract driverId from the key
            int bestDriverId = Integer.parseInt(bestDriverKey.split(":")[1]);

            // Prepare match result
            Map<String, Object> matchResult = new HashMap<>();
            matchResult.put("clientId", clientId);
            matchResult.put("driverId", bestDriverId);

            // Serialize match result to JSON and emit to match-stream
            try {
                collector.send(new OutgoingMessageEnvelope(MATCH_STREAM, matchResult));
            } catch (Exception e) {
                System.err.println("Failed to serialize match result: " + e.getMessage());
            }
        }

    }

    /**
     * Handle RIDE_COMPLETE type to update driver's location, rating, and
     * availability.
     *
     * @param event
     */
    private void handleRideComplete(JsonObject event) {
        int blockId = event.get("blockId").getAsInt();
        int driverId = event.get("driverId").getAsInt();
        double newLatitude = event.get("latitude").getAsDouble();
        double newLongitude = event.get("longitude").getAsDouble();
        double userRating = event.get("user_rating").getAsDouble();

        String key = constructKey(blockId, driverId);

        if (keyExists(key)) {
            Map<String, Object> driverInfo = driverStore.get(key);

            // Update location
            driverInfo.put("latitude", newLatitude);
            driverInfo.put("longitude", newLongitude);

            // Update rating: Simple average for demonstration
            double oldRating = (Double) driverInfo.get("rating");
            double updatedRating = (oldRating + userRating) / 2.0;
            driverInfo.put("rating", updatedRating);

            // Mark as AVAILABLE
            driverInfo.put("status", "AVAILABLE");

            // Persist the updated driver info
            driverStore.put(key, driverInfo);
        }
    }

    /**
     * composite key using blockId and driverId.
     *
     * @param blockId Identifier for the block
     * @param driverId Identifier for the driver
     * @return Composite key in the format "blockId:driverId"
     */
    private String constructKey(int blockId, int driverId) {
        return blockId + ":" + driverId;
    }

    /**
     * Computes the distance score between client and driver. A lower distance
     * results in a higher score.
     *
     * @param clientLat Client's latitude
     * @param clientLon Client's longitude
     * @param driverLat Driver's latitude
     * @param driverLon Driver's longitude
     * @return Distance-based score
     */
    private double computeDistanceScore(double clientLat, double clientLon, double driverLat, double driverLon) {
        double distance = Math.sqrt(Math.pow(clientLat - driverLat, 2) + Math.pow(clientLon - driverLon, 2));
        return Math.exp(-distance);
    }

    /**
     * Calculates the gender score based on driver's gender and client's
     * preference.
     *
     * @param driverGender Driver's gender
     * @param genderPreference Client's gender preference
     * @return Gender-based score
     */
    private double calculateGenderScore(String driverGender, String genderPreference) {
        if ("N".equalsIgnoreCase(genderPreference)) {
            return 1.0;
        }
        return driverGender.equalsIgnoreCase(genderPreference) ? 1.0 : 0.0;
    }

    /**
     * Check if the key exists in the kv store
     *
     * @param key
     * @return
     */
    public boolean keyExists(String key) {
        Map<String, Object> value = driverStore.get(key);
        return value != null;
    }
}
