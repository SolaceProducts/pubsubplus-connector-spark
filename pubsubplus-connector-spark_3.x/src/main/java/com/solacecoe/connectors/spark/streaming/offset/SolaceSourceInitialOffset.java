package com.solacecoe.connectors.spark.streaming.offset;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog;
import org.apache.spark.sql.execution.streaming.SerializedOffset;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class SolaceSourceInitialOffset extends HDFSMetadataLog {

    public SolaceSourceInitialOffset(SparkSession sparkSession, String path, ClassTag classTag$T$0) {
        super(sparkSession, path, classTag$T$0);
    }

    @Override
    public SolaceSourceOffset deserialize(InputStream inputStream) {
        // Read the content as a string (Support for Spark 2.1.0, as stated in the comment)
        try {
            inputStream.read(); // A zero byte is read to support Spark 2.1.0 (SPARK-19517)
            String content = IOUtils.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

            // HDFSMetadataLog guarantees that it never creates a partial file.
            if (content == null || content.isEmpty()) {
                throw new IllegalStateException("Log file is empty.");
            }

            if (content.charAt(0) == 'v') {
                // Log file versioning
                int indexOfNewLine = content.indexOf("\n");
                if (indexOfNewLine > 0) {
                    // Validate version
//                validateVersion(content.substring(0, indexOfNewLine), VERSION);
                    return new SolaceSourceOffset(new SerializedOffset(content.substring(indexOfNewLine + 1)));
                } else {
                    throw new IllegalStateException(
                            "Log file was malformed: failed to detect the log file version line.");
                }
            } else {
                return new SolaceSourceOffset(new SerializedOffset(content));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
