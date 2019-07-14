package sparkAssignment.GroupBy;

import java.util.Comparator;
import java.io.Serializable;

import scala.Tuple2;

public class TupleSorter implements Comparator<Tuple2<Integer,Long>>, Serializable {

	
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<Integer,Long> o1, Tuple2<Integer,Long> o2) {
		if (o1._1 > o2._1)
			return 1;
		else if (o1._1 < o2._1)
			return -1;
		else 
			return 0;
	}

}
