#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.jobs;



import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Arrays;

public class WordcountSimple {
    public static String createUri(String resourcePath) {
        try {
            return WordcountSimple.class.getResource(resourcePath).toURI().toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }

    }
    public static void run(String[] args){

        // Settings
        String inputUrl = createUri("/wordcount.txt");

        // Get a plan builder.
        RheemContext rheemContext = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(rheemContext)
                .withJobName(String.format("WordCount (%s)", inputUrl))
                .withUdfJarOf(WordcountSimple.class);

        // Start building the RheemPlan.
        Collection<Tuple2<String, Integer>> wordcounts = planBuilder
                // Read the text file.
                .readTextFile(inputUrl)

                // Split each line by non-word characters.
                .flatMap(line -> Arrays.asList(line.split("${symbol_escape}${symbol_escape}W+")))

                // Filter empty tokens.
                .filter(token -> !token.isEmpty())

                // Attach counter to each word.
                .map(word -> new Tuple2<>(word.toLowerCase(), 1))

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )

                // Execute the plan and collect the results.
                .collect();

        System.out.println(wordcounts);
    }
}