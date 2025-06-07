package org.cloudcomputing.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.cloudcomputing.enums.InternetStatus;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class InternetAccessReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final Map<String, int[]> resultMap = new LinkedHashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        String[] parts = key.toString().split("_");
        if (parts.length != 3) return;

        String combinedLabel = parts[0] + "_" + parts[1];
        InternetStatus internetStatus = InternetStatus.valueOf(parts[2]);

        int count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }

        int[] counts = resultMap.getOrDefault(combinedLabel, new int[2]);
        if (internetStatus == InternetStatus.HasInternetAccess) {
            counts[0] += count;
        } else {
            counts[1] += count;
        }
        resultMap.put(combinedLabel, counts);
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(
                null,
                new Text("|--------------------------------------------------------------------|")
        );
        context.write(
                null,
                new Text(String.format(
                        "| %-23s | %19s | %18s |", "Combined Feature Labels", "Has Internet Access", "No Internet Access")
                )
        );
        context.write(
                null,
                new Text("|-------------------------+---------------------+--------------------|")
        );

        for (Map.Entry<String, int[]> entry : resultMap.entrySet()) {
            String key = entry.getKey();
            int[] counts = entry.getValue();

            String formatted = String.format("| %-23s | %19s | %18s |",
                    key,
                    String.format("%,d", counts[0]),
                    String.format("%,d", counts[1]));
            context.write(null, new Text(formatted));
        }

        context.write(
                null,
                new Text("|--------------------------------------------------------------------|")
        );
    }
}
