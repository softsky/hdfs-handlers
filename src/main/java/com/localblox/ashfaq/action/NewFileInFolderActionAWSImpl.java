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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

/**
 * Action for processing input files with AWS.
 */
public class NewFileInFolderActionAWSImpl extends NewFileInFolderAction {

    private static final String STATUS_COMPLETE = "COMPLETED";
    private static final String BATCH_PREDICTION_NAME = "EXAMPLE";
    private static final String ML_MODEL_ID = "EXAMPLE-pr-2014-09-12-15-14-04-924";
    private static final String S3_OUTPUT_PATTERN = "s3://eml-test-EXAMPLE/test-outputs/%s/results";
    private static final String S3_DATA_LOCATION = "s3://eml-test-EXAMPLE/data.csv";
    private static final String S3_DATA_LOCATION_SCHEMA = "s3://eml-test-EXAMPLE/data.csv.schema";
    private static final String DATASOURCE_ID = "exampleDataSourceId";
    private static final String DATASOURCE_NAME = "exampleDataSourceName";
    @Override
    public void doIt(final String inFile) throws RuntimeException {
        // 1. Read file from HDFS IN folder
        SparkSession spark = getSparkSession();
        Dataset<Row> selectedData = readFromHDFS(spark, inFile);
        // init amazon machine learning client
        AmazonMachineLearning amazonMLClient = AmazonMachineLearningClientBuilder.defaultClient();
        // 3. Load input file to AWS datasource
        loadDataToS3(selectedData);
        // 2. Create DataSource (name from filename)
        GetDataSourceResult dataSource = createDataSource(amazonMLClient);
        // 4. Cretae Batch predition with params (DataSource ID, ...)
        GetBatchPredictionResult batchPredictionResult = createBatchPrediction(amazonMLClient, dataSource, inFile);

        // TODO 6. Read Prediction result from S3
        // TODO 7. write result into OUT folder in HDFS
    }

    private GetBatchPredictionResult createBatchPrediction(AmazonMachineLearning amazonMLClient,
                                                           GetDataSourceResult dataSource, String inFile) {
        CreateBatchPredictionRequest createBatchPredictionRequest = new CreateBatchPredictionRequest();
        createBatchPredictionRequest.withBatchPredictionId(inFile)
                                    .withBatchPredictionName(BATCH_PREDICTION_NAME)
                                    .withMLModelId(ML_MODEL_ID)
                                    .withBatchPredictionDataSourceId(dataSource.getDataSourceId())
                                    .withOutputUri(String.format(S3_OUTPUT_PATTERN, inFile));
        CreateBatchPredictionResult createBatchPredictionResult = amazonMLClient.createBatchPrediction(
            createBatchPredictionRequest);
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
        return batchPredictionResult;
    }

    private void loadDataToS3(Dataset<Row> selectedData) {
        selectedData.write().format("com.knoldus.spark.s3").option("accessKey", "s3_access_key")
                    .option("secretKey", "s3_secret_key").option("bucket", "bucket_name")
                    .option("fileType", "csv").save();
    }

    private GetDataSourceResult createDataSource(AmazonMachineLearning amazonMLClient) {
        CreateDataSourceFromS3Request createDataSourceFromS3Request = new CreateDataSourceFromS3Request();
        S3DataSpec dataSpec = new S3DataSpec();
        dataSpec.withDataLocationS3(S3_DATA_LOCATION).withDataSchemaLocationS3(
                S3_DATA_LOCATION_SCHEMA);
        createDataSourceFromS3Request.withDataSourceId(DATASOURCE_ID)
                                     .withDataSourceName(DATASOURCE_NAME).withDataSpec(dataSpec);
        CreateDataSourceFromS3Result dataSourceFromS3 = amazonMLClient
            .createDataSourceFromS3(createDataSourceFromS3Request);
        // check if created
        GetDataSourceRequest getDataSourceRequest = new GetDataSourceRequest()
            .withDataSourceId(dataSourceFromS3.getDataSourceId());
        GetDataSourceResult dataSource;
        do {
            dataSource = amazonMLClient.getDataSource(getDataSourceRequest);
            try {
                TimeUnit.MILLISECONDS.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } while (STATUS_COMPLETE.equals(dataSource.getStatus()));
        return dataSource;
    }

    private SparkSession getSparkSession() {
        return SparkSession.builder().appName("Java Spark SQL basic example")
                           .config("spark.master", "local").getOrCreate();
    }

    private Dataset<Row> readFromHDFS(SparkSession spark, String inFile) {
        Dataset<Row> ds = spark.read()
                               .csv(inFile)
                               .select("Address", "City Name", "State Code", "County Name", "Zip Code",
                                       "Contact Person Name", "Contact Person Position",
                                       "Gender Contact Person 2", "Employee Count",
                                       "Employee Range", "Annual Revenues", "Sales Range",
                                       "SIC Name / Category", "Category", "Full Category Set",
                                       "Latitude", "Longitude", "Physical Neighborhood",
                                       "Love Score", "Freshness Score", "Holistic Score",
                                       "Hours of Operation", "Reviews Scanned",
                                       "Good Reviews Scanned", "Average Review Rating",
                                       "Likes Count", "Social Media Profiles Count",
                                       "Facebook Profile", "Twitter Profile",
                                       "Foursquare Profile", "HQ_Followers", "HQ_Type",
                                       "HQ_Employees", "HQ_Name", "HQ_YearFounded",
                                       "HQ_Categories", "HQ_Specialties", "HQ_Revenue",
                                       "HQ_Ticker", "HQ_Exchange", "HQ_Acquisitions",
                                       "HQ_GrowthScore", "HQ_EstMonthlyUniques",
                                       "HQ_EstInboundLinks", "HQ_TwitterFollowers",
                                       "HQ_FacebookLikes", "HQ_FacebookTalkingAbout",
                                       "HQ_LinkedInFollowerCount");
        return ds;
    }
}
