package external_sort;

import java.util.Comparator;

public class StringComparator implements Comparator<String> {
	@Override
	public int compare(String s1, String s2) {
		return s1.compareToIgnoreCase(s2);
	}
}
