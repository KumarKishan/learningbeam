package net.fawad.learningbeam

import org.apache.beam.runners.flink.FlinkPipelineOptions
import org.apache.beam.runners.flink.FlinkRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory

fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).`as`(Options::class.java)
    options.runner = FlinkRunner::class.java

    val p = Pipeline.create(options)
    p.apply("Readlines", TextIO.Read.from(options.getInput()))
            .apply("WriteCounts", TextIO.Write.to(options.getOutput()))
    val result = p.run()
    println(result)

}

interface Options : PipelineOptions, FlinkPipelineOptions {
    @Description("File to read") fun getInput(): String
    fun setInput(x: String)
    @Description("File to write") fun getOutput(): String
    fun setOutput(x: String)
}
