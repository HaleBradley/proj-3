package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {

    public static class InteractionMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private final Text binPair = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] parts = line.split("\\s+");
            if (parts.length != 4) {
                return; // Invalid input row
            }

            try {
                int chrom1 = Integer.parseInt(parts[0]);
                int pos1 = Integer.parseInt(parts[1]);
                int chrom2 = Integer.parseInt(parts[2]);
                int pos2 = Integer.parseInt(parts[3]);

                int[] binInfo1 = getBinIndex(chrom1, pos1);
                int[] binInfo2 = getBinIndex(chrom2, pos2);

                if (binInfo1 == null || binInfo2 == null) {
                    return; // Skip invalid chromosome or position
                }

                int bin1 = binInfo1[1];
                int bin2 = binInfo2[1];

                // Ensure bin1 < bin2
                if (bin1 > bin2) {
                    int temp = bin1;
                    bin1 = bin2;
                    bin2 = temp;
                }

                binPair.set("(" + bin1 + "," + bin2 + ")");
                context.write(binPair, one);

            } catch (NumberFormatException e) {
                // Skip invalid rows
            }
        }

        private int[] getBinIndex(int chromosome, int position) {
            // Base pairs for each chromosome
            int[] chromosomeLengths = {
                    248956422, 242193529, 198295559, 190214555, 181538259,
                    170805979, 159345973, 145138636, 138394717, 133797422,
                    135086622, 133275309, 114364328, 107043718, 101991189,
                    90338345, 83257441, 80373285, 58617616, 64444167,
                    46709983, 50818468, 156040895 // Chromosome X
            };

            if (chromosome < 1 || chromosome > 23 || position < 1 || position > chromosomeLengths[chromosome - 1]) {
                return null; // Invalid chromosome or position
            }

            int bin = (int) Math.ceil(position / 100000.0);
            int binOffset = 0;
            for (int i = 0; i < chromosome - 1; i++) {
                binOffset += (int) Math.ceil(chromosomeLengths[i] / 100000.0);
            }

            return new int[]{chromosome, bin + binOffset};
        }
    }

    public static class InteractionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Interaction Counter");
        job.setJarByClass(App.class);
        job.setMapperClass(InteractionMapper.class);
        job.setCombinerClass(InteractionReducer.class);
        job.setReducerClass(InteractionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
