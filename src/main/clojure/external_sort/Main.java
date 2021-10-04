package external_sort;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class Main {
	public static void main(String[] args) throws IOException {
		Path fullPath = new File("/Users/lyc/Desktop/RA clojure/clojask/resources/", "Employees-large.csv").toPath();
		List<File> files = FileSplitSortMergeUtil.splitAndSortTempFiles(fullPath.toAbsolutePath().toString(), "/Users/lyc/Desktop/RA clojure/clojask/temp", 25,
				new StringComparator());
		FileSplitSortMergeUtil.mergeSortedFiles(files, "/Users/lyc/Desktop/RA clojure/clojask/resources/test.csv", new StringComparator());
	}
}