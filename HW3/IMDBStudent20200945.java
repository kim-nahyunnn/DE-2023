import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public final class IMDBStudent20200945 implements Serializable {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("IMDBStudent20200945")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        FlatMapFunction<String, String> fmf = new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
		String[] sString = s.split("::");
            	StringTokenizer itr = new StringTokenizer(sString[2], "|");
            	List<String> newList = new List<String>();
            	while(itr.hasMoreTokens()){
            		newList.add(itr.nextToken());
		}
                return Arrays.asList(newList).iterator();
            }
        };
        JavaRDD<String> words = lines.flatMap(fmf);

        PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2(s, 1);
            }
        };
        JavaPairRDD<String, Integer> ones = words.mapToPair(pf);

        Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        };
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
