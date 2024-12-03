package org.example.App;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class  App{
    
    // Static data
    private static final int[] CHROMOSOME_LENGTHS = {
        248956422, 242193529, 198295559, 190214555, 181538259,
        170805979, 159345973, 145138636, 138394717, 133797422,
        135086622, 133275309, 114364328, 107043718, 101991189,
        90338345, 83257441, 80373285, 58617616, 64444167,
        46709983, 50818468, 156040895 
    };

    // Mapper class
    public static class InteractionMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable SINGLE_OCCURRENCE = new IntWritable(1);
        private final Text binPairKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] interactions = parseInteractionLine(value.toString());
            if (interactions == null) return;

            try {
                int[] binInfo1 = calculateBinIndex(interactions[0], interactions[1]);
                int[] binInfo2 = calculateBinIndex(interactions[2], interactions[3]);

                if (binInfo1 == null || binInfo2 == null) return;

                int bin1 = binInfo1[1];
                int bin2 = binInfo2[1];

                //consistent bin pair ordering
                int orderedBin1 = Math.min(bin1, bin2);
                int orderedBin2 = Math.max(bin1, bin2);

                binPairKey.set(String.format("(%d,%d)", orderedBin1, orderedBin2));
                context.write(binPairKey, SINGLE_OCCURRENCE);

            } catch (NumberFormatException e) {
                //ignore invalid number formats
            }
        }

        //method to parse input line
        private String[] parseInteractionLine(String line) {
            String[] parts = line.trim().split("\\s+");
            return parts.length == 4 ? parts : null;
        }

        //bin index for a chromosome and position
        private int[] calculateBinIndex(String chromosomeStr, String positionStr) {
            int chromosome = Integer.parseInt(chromosomeStr);
            int position = Integer.parseInt(positionStr);

            if (!isValidChromosomePosition(chromosome, position)) {
                return null;
            }

            int binsPerChromosome = (int) Math.ceil(CHROMOSOME_LENGTHS[chromosome - 1] / 100000.0);
            int binWithinChromosome = (int) Math.ceil(position / 100000.0);
            
            int binOffset = calculateBinOffset(chromosome);
            int globalBin = binWithinChromosome + binOffset;

            return new int[]{chromosome, globalBin};
        }

        private boolean isValidChromosomePosition(int chromosome, int position) {
            return chromosome >= 1 && chromosome <= 23 && 
                   position >= 1 && position <= CHROMOSOME_LENGTHS[chromosome - 1];
        }

        //compute offset for earlier chromosomes
        private int calculateBinOffset(int chromosome) {
            int binOffset = 0;
            for (int i = 0; i < chromosome - 1; i++) {
                binOffset += (int) Math.ceil(CHROMOSOME_LENGTHS[i] / 100000.0);
            }
            return binOffset;
        }
    }

    // Reducer class
    public static class InteractionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable aggregatedResult = new IntWritable();

        @Override
        protected void reduce(Text binPair, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int totalInteractionCount = 0;
            
            for (IntWritable singleInteraction : values) {
                totalInteractionCount += singleInteraction.get();
            }
            
            aggregatedResult.set(totalInteractionCount);
            context.write(binPair, aggregatedResult);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job genomicInteractionJob = Job.getInstance(configuration, "Genomic Interaction Counter");
        
        genomicInteractionJob.setJarByClass(GenomicInteractionAnalyzer.class);
        genomicInteractionJob.setMapperClass(InteractionMapper.class);
        genomicInteractionJob.setCombinerClass(InteractionReducer.class);
        genomicInteractionJob.setReducerClass(InteractionReducer.class);
        
        genomicInteractionJob.setOutputKeyClass(Text.class);
        genomicInteractionJob.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(genomicInteractionJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(genomicInteractionJob, new Path(args[1]));
        
        System.exit(genomicInteractionJob.waitForCompletion(true) ? 0 : 1);
    }
}