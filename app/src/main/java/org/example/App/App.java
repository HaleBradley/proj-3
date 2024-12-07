package org.example.App;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class App {
    private static final int[] CHROMOSOME_LENGTHS = {
        248956422, 242193529, 198295559, 190214555, 181538259,
        170805979, 159345973, 145138636, 138394717, 133797422,
        135086622, 133275309, 114364328, 107043718, 101991189,
        90338345, 83257441, 80373285, 58617616, 64444167,
        46709983, 50818468, 156040895
    };

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: App <input_file> <output_dir>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder().appName("Genomic Interaction Counter").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaPairRDD<String, Integer> binPairs = lines
            .mapToPair(line -> {
                String[] interactions = line.split("\\s+");
                if (interactions.length != 4) return null;

                int[] binInfo1 = calculateBinIndex(interactions[0], interactions[1]);
                int[] binInfo2 = calculateBinIndex(interactions[2], interactions[3]);

                if (binInfo1 == null || binInfo2 == null) return null;

                int bin1 = binInfo1[1];
                int bin2 = binInfo2[1];
                int orderedBin1 = Math.min(bin1, bin2);
                int orderedBin2 = Math.max(bin1, bin2);

                return new Tuple2<>(String.format("(%d,%d)", orderedBin1, orderedBin2), 1);
            })
            .filter(pair -> pair != null);

        JavaPairRDD<String, Integer> binCounts = binPairs.reduceByKey(Integer::sum);
        binCounts.saveAsTextFile(args[1]);

        spark.stop();
    }

    private static int[] calculateBinIndex(String chromosomeStr, String positionStr) {
        try {
            int chromosome = Integer.parseInt(chromosomeStr);
            int position = Integer.parseInt(positionStr);

            if (chromosome < 1 || chromosome > 23 || position < 1 || position > CHROMOSOME_LENGTHS[chromosome - 1]) {
                return null;
            }

            int binsPerChromosome = (int) Math.ceil(CHROMOSOME_LENGTHS[chromosome - 1] / 100000.0);
            int binWithinChromosome = (int) Math.ceil(position / 100000.0);

            int binOffset = 0;
            for (int i = 0; i < chromosome - 1; i++) {
                binOffset += (int) Math.ceil(CHROMOSOME_LENGTHS[i] / 100000.0);
            }

            return new int[]{chromosome, binWithinChromosome + binOffset};
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
