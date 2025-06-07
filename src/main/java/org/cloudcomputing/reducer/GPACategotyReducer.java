package org.cloudcomputing.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.cloudcomputing.enums.GPACategory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class GPACategotyReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final Map<String, String[]> resultMap = new LinkedHashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        String[] parts = key.toString().split("_");
        if (parts.length != 3) return;

        String combinedLabel = parts[0] + "_" + parts[1];
        GPACategory gpaClass = GPACategory.valueOf(parts[2]);

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        resultMap.putIfAbsent(combinedLabel, new String[]{"0", "0", "0", "0"});

        switch (gpaClass) {
            case FirstClass:
                resultMap.get(combinedLabel)[0] = String.valueOf(sum);
                break;
            case SecondClassUpper:
                resultMap.get(combinedLabel)[1] = String.valueOf(sum);
                break;
            case SecondClassLower:
                resultMap.get(combinedLabel)[2] = String.valueOf(sum);
                break;
            case Normal:
                resultMap.get(combinedLabel)[3] = String.valueOf(sum);
                break;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(
                new Text("|-------------------------------------------------------------------------------------------|"),
                null
        );
        // Header
        context.write(new Text(String.format("| %-35s | %10s | %12s | %12s | %8s |",
                        "Combined Feature Labels", "FirstClass", "SecondUpper", "SecondLower", "Normal")),
                null);
        context.write(
                new Text("|-------------------------------------+------------+--------------+--------------+----------|"),
                null
        );

        // Rows
        for (Map.Entry<String, String[]> entry : resultMap.entrySet()) {
            String label = entry.getKey();
            String[] counts = entry.getValue();
            context.write(new Text(String.format("| %-35s | %10s | %12s | %12s | %8s |",
                    label,
                    String.format("%,d", Integer.parseInt(counts[0])),
                    String.format("%,d", Integer.parseInt(counts[1])),
                    String.format("%,d", Integer.parseInt(counts[2])),
                    String.format("%,d", Integer.parseInt(counts[3]))
            )), null);
        }

        context.write(
                new Text("|-------------------------------------------------------------------------------------------|"),
                null
        );
    }
}
