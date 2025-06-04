package org.cloudcomputing.tableformatter.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class InternetAccessReducer extends Reducer<Text, Text, Text, Text> {
    private final Map<String, int[]> resultMap = new LinkedHashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {
        int internetAccess = 0;
        int noAccess = 0;

        for (Text val : values) {
            if (val.toString().equals("InternetAccess")) {
                internetAccess++;
            } else {
                noAccess++;
            }
        }

        resultMap.put(key.toString(), new int[]{internetAccess, noAccess});
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(null, new Text("|--------------------------------------------------------------------|"));
        context.write(null, new Text(String.format(
                "| %-23s | %19s | %18s |", "Combined Feature Labels", "Has Internet Access", "No Internet Access")));
        context.write(null, new Text("|-------------------------|---------------------|--------------------|"));

        for (Map.Entry<String, int[]> entry : resultMap.entrySet()) {
            String key = entry.getKey();
            int[] counts = entry.getValue();

            String formatted = String.format("| %-23s | %19s | %18s |",
                    key,
                    String.format("%,d", counts[0]),
                    String.format("%,d", counts[1]));
            context.write(null, new Text(formatted));
        }
        context.write(null, new Text("|--------------------------------------------------------------------|"));
    }
}
