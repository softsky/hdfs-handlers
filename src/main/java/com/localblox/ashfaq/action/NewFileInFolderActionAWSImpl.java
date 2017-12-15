package com.localblox.ashfaq.action;

import com.amazonaws.services.machinelearning.AmazonMachineLearning;
import com.amazonaws.services.machinelearning.AmazonMachineLearningClientBuilder;
import com.amazonaws.services.machinelearning.model.CreateBatchPredictionRequest;
import com.amazonaws.services.machinelearning.model.CreateBatchPredictionResult;
import com.amazonaws.services.machinelearning.model.CreateDataSourceFromS3Request;
import com.amazonaws.services.machinelearning.model.CreateDataSourceFromS3Result;
import com.amazonaws.services.machinelearning.model.GetBatchPredictionRequest;
import com.amazonaws.services.machinelearning.model.GetBatchPredictionResult;
import com.amazonaws.services.machinelearning.model.GetDataSourceRequest;
import com.amazonaws.services.machinelearning.model.GetDataSourceResult;
import com.amazonaws.services.machinelearning.model.S3DataSpec;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3EncryptionClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * Action for processing input files with AWS.
 */
public class NewFileInFolderActionAWSImpl extends NewFileInFolderAction {

    private static final String STATUS_COMPLETE = "COMPLETED";

    // TODO split the logic into separate methods of the class
    @Override
    public void doIt(final String inFile) throws RuntimeException {
        // 1. Read file from HDFS IN folder
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();
        Dataset<Row> dataSet = spark.read().csv(inFile);
        // init amazon machine learning client
        AmazonMachineLearning amazonMLClient = AmazonMachineLearningClientBuilder.defaultClient();
        // 2. Create DataSource (name from filename)
        CreateDataSourceFromS3Request createDataSourceFromS3Request = new CreateDataSourceFromS3Request();
        S3DataSpec dataSpec = new S3DataSpec();
        dataSpec.withDataLocationS3("s3://eml-test-EXAMPLE/data.csv").withDataSchemaLocationS3(
                        "s3://eml-test-EXAMPLE/data.csv.schema");
        createDataSourceFromS3Request.withDataSourceId("exampleDataSourceId")
                        .withDataSourceName(inFile).withDataSpec(dataSpec);
        CreateDataSourceFromS3Result dataSourceFromS3 = amazonMLClient.createDataSourceFromS3(createDataSourceFromS3Request);
        // check if created
        GetDataSourceRequest getDataSourceRequest = new GetDataSourceRequest().withDataSourceId(dataSourceFromS3.getDataSourceId());
        GetDataSourceResult dataSource;
        do {
            dataSource = amazonMLClient.getDataSource(getDataSourceRequest);
            try {
                TimeUnit.MILLISECONDS.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } while (STATUS_COMPLETE.equals(dataSource.getStatus()));

        // 3. Load input file to AWS datasource
        // TODO convert Spark's DataSet to inputStream
        AmazonS3 s3client = AmazonS3EncryptionClientBuilder.defaultClient();
        InputStream inputStream = null;
        ObjectMetadata objectMetadata = null;
        s3client.putObject(new PutObjectRequest("bucketName", "keyName", inputStream, objectMetadata));

        // 4. Cretae Batch predition with params (DataSource ID, ...)
        CreateBatchPredictionRequest createBatchPredictionRequest = new CreateBatchPredictionRequest();
        createBatchPredictionRequest.withBatchPredictionId("EXAMPLE-bp-2014-09-12-15-14-04-156")
                        .withBatchPredictionName("EXAMPLE")
                        .withMLModelId("EXAMPLE-pr-2014-09-12-15-14-04-924")
                        .withBatchPredictionDataSourceId("EXAMPLE-tr-ds-2014-09-12-15-14-04-989")
                        .withOutputUri("s3://eml-test-EXAMPLE/test-outputs/EXAMPLE-bp-2014-09-12-15-14-04-156/results");
        // execute request
        CreateBatchPredictionResult createBatchPredictionResult = amazonMLClient.createBatchPrediction(createBatchPredictionRequest);
        String batchPredictionId = createBatchPredictionResult.getBatchPredictionId();
        // 5. Poll predtion status untill COMPLETE
        GetBatchPredictionRequest getBatchPredictionRequest = new GetBatchPredictionRequest();
        getBatchPredictionRequest.withBatchPredictionId(batchPredictionId);
        GetBatchPredictionResult batchPredictionResult;
        do {
            batchPredictionResult = amazonMLClient.getBatchPrediction(getBatchPredictionRequest);
            try {
                TimeUnit.MILLISECONDS.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } while (!STATUS_COMPLETE.equals(batchPredictionResult.getStatus()));

        // TODO 6. Read Prediction result from S3
        // TODO 7. write result into OUT folder in HDFS
    }

}
