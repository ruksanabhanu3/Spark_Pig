package sparkAssignment.Filter;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Filter {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Filter");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> inputDataRdd = sc.textFile(args[0]);

		//Removing Headers
		JavaRDD<String> inputDataNoHeaderRdd = inputDataRdd.filter(x -> !(x.contains("VendorID")));
		
		//Filtering with ratecode ==4
		JavaRDD<String> filterRdd = inputDataNoHeaderRdd.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				if (v1.length() >= 5 && v1.charAt(0) == ',')
					return false;
				else {
					String v11[] = v1.split(",");
					return ((v1.length() >= 5) ? (Integer.parseInt(v11[5]) == 4 ? true : false) : false);
				}
			}

		});

		List<String> finalList = filterRdd.collect();

		//Saving to text file.
		filterRdd.saveAsTextFile(args[1]);
		
		sc.close();
	}

}
