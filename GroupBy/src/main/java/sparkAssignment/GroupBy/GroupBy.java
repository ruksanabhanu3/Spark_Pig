package sparkAssignment.GroupBy;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class GroupBy {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> inputDataRdd = sc.textFile(args[0]);
		//JavaRDD<String> inputDataRdd = sc.textFile("C:\\USERS\\RUKSANA\\Desktop\\SparkRelated\\yellow_tripdata_2017-07.csv");
		
		//Remove Header
		JavaRDD<String> inputDataNoHeaderRdd = inputDataRdd.filter(x -> !(x.contains("VendorID")));
		
		//Paired Rdd with (payment type,1) as a pair. in case of any Exception (0,1) will be a pair
		JavaPairRDD<Integer, Long> pairRdd = inputDataNoHeaderRdd.mapToPair(new PairFunction<String, Integer, Long>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<Integer, Long> call(String record) throws Exception {
				if (!(record.length() > 0 && record.charAt(0) == ',')) {
					String[] record_values = record.split(",");
					if (record_values.length >= 9)
						return ((record_values.length >= 9
								? new Tuple2<Integer, Long>(Integer.parseInt(record_values[9]), (long) 1)
								: new Tuple2<Integer, Long>(0, (long) 1)));
				}
				return new Tuple2<Integer, Long>(0, (long) 1);
			}
		});

		//Reducing the paired RDD using reduce by key
		JavaPairRDD<Integer, Long> ReduceCntRdd = pairRdd.reduceByKey((x, y) -> x + y);
		
		//Filtering out Exceptional pairs such as (0,1)
		JavaPairRDD<Integer, Long> ReduceCntRdd1 = ReduceCntRdd.filter(new Function<Tuple2<Integer, Long>, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(Tuple2<Integer, Long> v1) throws Exception {
				if (v1._1 == 0)
					return false;
				else
					return true;
			}
		});

		//Sorting
		List<Tuple2<Integer, Long>> sortedList = ReduceCntRdd1.top((int) ReduceCntRdd1.count(), new TupleSorter());

		JavaRDD<Tuple2<Integer, Long>> finalRdd = sc.parallelize(sortedList, 1);

		List<Tuple2<Integer, Long>> finalList = finalRdd.collect();
		
		//Saving to file
		finalRdd.saveAsTextFile(args[1]);

		for (Tuple2<Integer, Long> t : finalList) {
			System.out.println(t);
		}

		sc.close();

	}}
