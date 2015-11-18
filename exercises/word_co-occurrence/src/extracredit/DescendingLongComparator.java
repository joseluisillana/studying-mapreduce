package extracredit;

import org.apache.hadoop.io.LongWritable.Comparator;
import org.apache.hadoop.io.WritableComparable;


/**
 * This class compares two LongWritables.  It inherites from the 
 * default Comparator for LongWritables, but reverses the comparison,
 * so that it sorts in descending instead of ascending order.
 */
public class DescendingLongComparator extends Comparator {

	public DescendingLongComparator() {
		super();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable v1, WritableComparable v2){
	
		return -1 * super.compare(v1, v2);

	}	
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		return -1 * super.compare(b1,s1,l1,b2,s2,l2);
	}

}
