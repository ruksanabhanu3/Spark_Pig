package sparkAssignment.LookUp;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SingleLookUp {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SingleLookUp");
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> inputDataRdd = sc.textFile(args[0]);

		//Removing Headers
		JavaRDD<String> inputDataNoHeaderRdd = inputDataRdd.filter(x -> !(x.contains("VendorID")));

		//Filtering as per the paramters given in problem statement
		JavaRDD<String> lookUpRdd = inputDataNoHeaderRdd.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String record) throws Exception {
				if (record.length() > 0 && record.charAt(0) == ',')
					return false;
				else {
					String vals[] = record.split(",");
					DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

					if ((vals[0]).equals("2") 
						&& (sdf.parse(vals[1])).equals(sdf.parse("2017-10-01 00:15:30"))
						&& (sdf.parse(vals[2])).equals(sdf.parse("2017-10-01 00:25:11"))
						&& (vals[3]).equals("1")
						&& (vals[4]).equals("2.17"))
						return true;
					else
						return false;
				}
			}
		}

		);
		
		List<String> inputDataNoHeaderRddList = lookUpRdd.collect();
		
		//Saving to the file
		lookUpRdd.saveAsTextFile(args[1]);

		for (String i : inputDataNoHeaderRddList) {
			System.out.println(i);

		}
	}

}
