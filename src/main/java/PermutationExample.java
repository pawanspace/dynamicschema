import com.google.common.collect.Collections2;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by pawanc on 6/29/18.
 */
public class PermutationExample {

    public static void main(String[] args) {
        Collection<List<String>> permutations = Collections2
                .orderedPermutations(Arrays.asList("A", "B", "C"));

        permutations.forEach(permutation -> {
            System.out.println(permutation);
        });
    }

}
