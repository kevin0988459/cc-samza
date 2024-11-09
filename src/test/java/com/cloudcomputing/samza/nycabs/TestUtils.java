package com.cloudcomputing.samza.nycabs;

import com.google.common.io.Resources;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestUtils {
    public static List<Map<String, Object>> genStreamData(String channel) {
        List<String> streamDataRawStrings = null;
        if (channel.equals("events")) {
            streamDataRawStrings = readFile("events.txt");
        } else if (channel.equals("driver-locations")) {
            streamDataRawStrings = readFile("driverLocations.txt");
        }

        return streamDataRawStrings.stream().map(s -> {
            Map<String, Object> result;
            ObjectMapper mapper = new ObjectMapper();
            try {
                result =  mapper.readValue(s, HashMap.class);
                return result;
            } catch (Exception e) {
                System.out.println("Failed in parse " + s + ", skip into next line");
            }
            return null;
        }).filter(x -> x != null).collect(Collectors.toList());
    }

    private static List<String> readFile(String path) {
        try {
            InputStream in = Resources.getResource(path).openStream();
            List<String> lines = new ArrayList<>();
            String line = null;
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            reader.close();
            return lines;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
