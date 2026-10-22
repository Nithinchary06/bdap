// EXPERIMENT 4 — UBER BUSIEST DATE BY DISPATCHING BASE
// Aim: Find the date(s) with the maximum trip count for each dispatching base.
// CSV columns must be: dispatching_base_num,date,active_vehicles,trips.
// Save as UberMaxTrips.java and build with Hadoop libraries.

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UberMaxTrips {
    public static class UberMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            // Skip blank lines and the CSV header.
            if (line.isEmpty() || line.toLowerCase().startsWith("dispatching_base_num")) return;
            String[] col = line.split(",", -1); // Simple CSV split; assumes no quoted commas.
            if (col.length < 4) return;

            String base = col[0].trim();
            String date = col[1].trim();
            try {
                int trips = Integer.parseInt(col[3].trim());
                // Send date and trip count as one value for this base.
                context.write(new Text(base), new Text(date + "\t" + trips));
            } catch (NumberFormatException e) {
                // Ignore malformed or non-numeric rows.
            }
        }
    }

    public static class UberReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text base, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int maxTrips = Integer.MIN_VALUE;
            List<String> bestDates = new ArrayList<>();

            // Track the maximum and preserve all dates tied at that maximum.
            for (Text value : values) {
                String[] parts = value.toString().split("\\t", 2);
                if (parts.length != 2) continue;
                String date = parts[0];
                try {
                    int trips = Integer.parseInt(parts[1]);
                    if (trips > maxTrips) {
                        maxTrips = trips;
                        bestDates.clear();
                        bestDates.add(date);
                    } else if (trips == maxTrips) {
                        bestDates.add(date);
                    }
                } catch (NumberFormatException e) {
                    // Skip invalid trip counts.
                }
            }
            Collections.sort(bestDates);
            if (maxTrips != Integer.MIN_VALUE) {
                context.write(base, new Text("date(s)=" + bestDates + ", maxTrips=" + maxTrips));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: UberMaxTrips <input-path> <output-path>");
            System.exit(2);
        }
        Job job = Job.getInstance(new Configuration(), "Uber busiest date by base");
        job.setJarByClass(UberMaxTrips.class);
        job.setMapperClass(UberMapper.class);
        job.setReducerClass(UberReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
EXECUTION:
  hdfs dfs -mkdir -p /uber/input
  hdfs dfs -put uber.csv /uber/input/
  # Build ubermax.jar with the class above and Hadoop dependencies.
  hadoop jar ubermax.jar UberMaxTrips /uber/input /uber/output
  hdfs dfs -cat /uber/output/part-r-00000

EXPECTED CHECK: One output row per base, with maximum trip count and all tied dates.
The output directory must be new for each run.
*/
