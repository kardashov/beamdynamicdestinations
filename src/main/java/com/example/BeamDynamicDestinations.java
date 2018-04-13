package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Requirements;

import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;
import static org.apache.beam.sdk.transforms.Contextful.fn;

/**
 * <p>
 * Run the example with
 * 
 * <pre>
 * mvn compile exec:java -Dexec.mainClass=org.example.BeamDynamicDestinations -Dexec.args="--runner=DirectRunner" -Pdirect-runner
 * </pre>
 */
public class BeamDynamicDestinations {
	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
		Pipeline p = Pipeline.create(options);

		p.apply(Create.of(
				          "dir1_subdir1_word1","dir1_subdir1_word2", "dir1_subdir1_word5",
				          	"dir1_subdir2_word2","dir1_subdir2_word2",
				          "dir2_subdir1_word3","dir2_subdir1_word4","dir2_subdir1_word4",
				            "dir2_subdir2_word4","dir2_subdir2_word4","dir2_subdir2_word4",
				          "dir3_subdir1_word6"))

				.apply(FileIO.<String, String>writeDynamic()
						.by(fn((element, c) -> {
							System.out.println(">>>>"+element);
							return element.substring(0, 12); //element-based destination partitioning function 
						}, Requirements.empty()))
						.via(TextIO.sink().withHeader("My header"))
						.to("dynamic_results_path")
						.withDestinationCoder(StringUtf8Coder.of())
						.withNumShards(1)
						//Destination naming rules
						.withNaming(dest -> defaultNaming(dest.substring(0,4) + "/" + dest.substring(5,12) + "/" + "words", ".csv")));
		p.run().waitUntilFinish();
	}
}
