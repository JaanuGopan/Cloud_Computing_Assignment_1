package org.cloudcomputing.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class GPAClassReducer extends Reducer<Text, Text, Text, Text> {

    private final Map<String, String[]> resultMap = new LinkedHashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        try {
            String[] parts = key.toString().split("_");
            if (parts.length != 3) return;

            String genderSchool = parts[0] + "_" + parts[1];  // e.g., Male_Private
            String gpaClass = parts[2];                       // e.g., FirstClass

            int sum = 0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }

            // Initialize row if not already present
            resultMap.putIfAbsent(genderSchool, new String[]{"0", "0", "0", "0"});

            // Update corresponding GPA class count
            switch (gpaClass) {
                case "FirstClass":
                    resultMap.get(genderSchool)[0] = String.valueOf(sum);
                    break;
                case "SecondUpper":
                    resultMap.get(genderSchool)[1] = String.valueOf(sum);
                    break;
                case "SecondLower":
                    resultMap.get(genderSchool)[2] = String.valueOf(sum);
                    break;
                case "Normal":
                    resultMap.get(genderSchool)[3] = String.valueOf(sum);
                    break;
            }
        } catch (Exception ignored) {}
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("|--------------------------------+------------+-------------+-------------+----------|"), null);
        // Print header
        context.write(new Text(String.format("| %-30s | %10s | %11s | %11s | %8s |",
                        "Combined Feature Labels", "FirstClass", "SecondUpper", "SecondLower", "Normal")),
                null);
        context.write(new Text("|--------------------------------+------------+-------------+-------------+----------|"), null);

        // Print rows
        for (Map.Entry<String, String[]> entry : resultMap.entrySet()) {
            String genderSchool = entry.getKey();
            String[] counts = entry.getValue();
            String line = String.format("| %-30s | %10s | %11s | %11s | %8s |",
                    genderSchool,
                    String.format("%,d", Integer.parseInt(counts[0])),
                    String.format("%,d", Integer.parseInt(counts[1])),
                    String.format("%,d", Integer.parseInt(counts[2])),
                    String.format("%,d", Integer.parseInt(counts[3]))
            );
            context.write(new Text(line), null);
        }

        context.write(new Text("|--------------------------------+------------+-------------+-------------+----------|"), null);
    }
}
