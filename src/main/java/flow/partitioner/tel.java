package flow.partitioner;

public class tel {
	public static void main(String[] args) {
		int[] arr = new int[]{5, 6, 1, 0, 3};
		int[] index = new int[]{2, 0, 3, 4, 1, 2, 1, 3, 2, 1, 4};
		String tel = " ";
		for (int i : index) {
			tel += arr[i];
		}
		System.out.println(tel);
	}
}
