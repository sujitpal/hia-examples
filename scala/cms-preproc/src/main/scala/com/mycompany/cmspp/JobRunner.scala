package com.mycompany.cmspp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.ToolRunner
import com.twitter.scalding.Tool

/**
 * Entrypoint for Hadoop to kick off the job.
 * Borrowed from com.twitter.scalding.Tool
 */
object JobRunner {
  def main(args: Array[String]) {
    ToolRunner.run(new Configuration, new Tool, args);
  }
}