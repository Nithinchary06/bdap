// EXPERIMENT 3 — MAPREDUCE WORD FREQUENCY
// Aim: Count the frequency of every word in an input text file.
// Save as WordCount.java. Build/package against the Hadoop libraries installed in your lab.

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Tokenize each input line and emit (word, 1).
            StringTokenizer tokens = new StringTokenizer(value.toString());
            while (tokens.hasMoreTokens()) {
                word.set(tokens.nextToken());
                context.write(word, ONE);
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Sum the counts for this word.
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // Supply HDFS input path and a new HDFS output path as arguments.
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input-path> <output-path>");
            System.exit(2);
        }
        Job job = Job.getInstance(new Configuration(), "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
EXECUTION (Linux terminal; adapt JAR build steps to your lab):
  echo "hello world" > input.txt
  echo "hello hadoop" >> input.txt
  hdfs dfs -mkdir -p /input
  hdfs dfs -put input.txt /input/
  # Build wc.jar using Maven/Gradle or the classpath method taught in your lab.
  hadoop jar wc.jar WordCount /input /output
  hdfs dfs -cat /output/part-r-00000

EXPECTED CHECK:
  hello 2
  world 1
  hadoop 1

IMPORTANT: /output must not already exist. Delete an old output directory before rerunning.
*/
