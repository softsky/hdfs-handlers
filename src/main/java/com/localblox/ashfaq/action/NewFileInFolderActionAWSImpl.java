package com.localblox.ashfaq.action;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_extract;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.when;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
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
import com.localblox.ashfaq.config.AppConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Action for processing input files with AWS.
 */
public class NewFileInFolderActionAWSImpl extends NewFileInFolderAction {

    private static final Logger log = LoggerFactory.getLogger(NewFileInFolderActionAWSImpl.class);

    private static final String STATUS_COMPLETE = "COMPLETED";
    private static final String BATCH_PREDICTION_NAME = "EXAMPLE";
    private static final String ML_MODEL_ID = "EXAMPLE-pr-2014-09-12-15-14-04-924";
    private static final String S3_OUTPUT_PATTERN = "s3://eml-test-EXAMPLE/test-outputs/%s/results"; // TODO get from
    // AppConfig
    private static final String S3_DATA_LOCATION = "s3://eml-test-EXAMPLE/data.csv"; // TODO get from AppConfig
    private static final String S3_DATA_LOCATION_SCHEMA = "s3://eml-test-EXAMPLE/data.csv.schema"; // TODO get from
    // AppConfig
    private static final String DATASOURCE_ID = "exampleDataSourceId";
    private static final String DATASOURCE_NAME = "exampleDataSourceName";

    private static final String UDF_VECTOR_TO_STRING = "udfVectorToSting";

    @Override
    public void doIt(final String inFile) throws RuntimeException {
        // 1. Read file from HDFS IN folder
        SparkSession spark = getSparkSession();
        Dataset<Row> selectedData = readFromHDFS(spark, inFile);
        // init amazon machine learning client
        AmazonMachineLearning amazonMLClient = getAmazonMLClient();
        // 3. Load input file to AWS datasource
        loadDataToS3(selectedData);
        // 2. Create DataSource (name from filename)
        GetDataSourceResult dataSource = createDataSource(amazonMLClient);
        // 4. Cretae Batch predition with params (DataSource ID, ...)
        GetBatchPredictionResult batchPredictionResult = createBatchPrediction(amazonMLClient, dataSource, inFile);

        // TODO 6. Read Prediction result from S3
        // TODO 7. write result into OUT folder in HDFS
    }

    /**
     * Init Amazon ML client with credentials and for US_EAST_1 region.
     */
    private AmazonMachineLearning getAmazonMLClient() {
        return AmazonMachineLearningClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                AppConfig.getInstance().getS3AccessKeyId(),
                AppConfig.getInstance().getS3SecretAccessKey())))
            .withRegion(Regions.US_EAST_1)
            .build();
    }

    /**
     * Send request to AWS ML to create batch prediction, then wait for creation and return prediction instance.
     *
     * @param client     - AWS client
     * @param dataSource - Data source
     * @param inFile     - input file name
     */
    private GetBatchPredictionResult createBatchPrediction(AmazonMachineLearning client,
                                                           GetDataSourceResult dataSource,
                                                           String inFile) {
        CreateBatchPredictionRequest createBatchPredictionRequest = new CreateBatchPredictionRequest();
        createBatchPredictionRequest.withBatchPredictionId(inFile)
                                    .withBatchPredictionName(BATCH_PREDICTION_NAME)
                                    .withMLModelId(ML_MODEL_ID)
                                    .withBatchPredictionDataSourceId(dataSource.getDataSourceId())
                                    .withOutputUri(String.format(S3_OUTPUT_PATTERN, inFile));

        CreateBatchPredictionResult createBatchPredictionResult = client.createBatchPrediction(
            createBatchPredictionRequest);
        String batchPredictionId = createBatchPredictionResult.getBatchPredictionId();

        // 5. Poll predtion status until COMPLETE
        GetBatchPredictionRequest getBatchPredictionRequest = new GetBatchPredictionRequest();
        getBatchPredictionRequest.withBatchPredictionId(batchPredictionId);

        GetBatchPredictionResult batchPredictionResult;
        do {
            batchPredictionResult = client.getBatchPrediction(getBatchPredictionRequest);
            try {
                TimeUnit.MILLISECONDS.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } while (!STATUS_COMPLETE.equals(batchPredictionResult.getStatus()));
        return batchPredictionResult;
    }

    private void loadDataToS3(Dataset<Row> selectedData) {
        log.info("Starting to load data to S3");
        //TODO - delete after connect issues will be resolved
/*        AmazonS3 client = AmazonS3ClientBuilder
                        .standard()
                        .withRegion(Regions.US_EAST_1)
                        .withCredentials(
                                        new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                                                        AppConfig.getInstance().getS3AccessKeyId(),
                                                        AppConfig.getInstance()
                                                                        .getS3SecretAccessKey())))
                        .build();

        client.put*/

        String accKey = null;
        String secretKey = null;
        try {
            accKey = URLEncoder.encode(AppConfig.getInstance().getS3AccessKeyId(), "UTF-8");
            secretKey = URLEncoder.encode(AppConfig.getInstance().getS3SecretAccessKey(), "UTF-8");

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        // TODO - such approach only for debug!!! it is not secure.
        // there are better approaches: https://hadoop.apache.org/docs/r2.9.0/hadoop-aws/tools/hadoop-aws/index
        // .html#S3A_Authentication_methods
        String s3path = "s3a://" + accKey + ":" + secretKey + "@b-data/";
        selectedData.write().csv(s3path);

        //TODO - delete after connect issues will be resolved
        /*format("com.databricks.spark.csv")
                    .option("accessKey", AppConfig.getInstance().getS3AccessKeyId())
                    .option("secretKey", AppConfig.getInstance().getS3SecretAccessKey())
                    .option("bucket", "bucket_name")
                    .option("fileType", "csv")
                    .save();*/
    }

    private GetDataSourceResult createDataSource(AmazonMachineLearning amazonMLClient) {

        CreateDataSourceFromS3Request createDataSourceFromS3Request = new CreateDataSourceFromS3Request();
        S3DataSpec dataSpec = new S3DataSpec();

        dataSpec.withDataLocationS3(S3_DATA_LOCATION)
                .withDataSchemaLocationS3(S3_DATA_LOCATION_SCHEMA);

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
        return SparkSession.builder().appName("Input file to AWS ML prediction action").getOrCreate();
    }

    private Dataset<Row> readFromHDFS(SparkSession spark, String inFile) {

        StopWatch watch = new StopWatch();
        watch.start();

        registerUdfs(spark);

        Dataset<Row> ds = spark.read()
                               .option("header", true)
                               .csv(inFile)
                               .select("LocalBlox ID", "Address", "City Name",
                                       "State Code", "County Name", "Zip Code",
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
                                       "HQ_Ticker", /*"HQ_Exchange",*/ "HQ_Acquisitions",
                                       "HQ_GrowthScore", "HQ_EstMonthlyUniques",
                                       "HQ_EstInboundLinks", "HQ_TwitterFollowers",
                                       "HQ_FacebookLikes", "HQ_FacebookTalkingAbout",
                                       "HQ_LinkedInFollowerCount",
                                       "Age on LocalBlox",
                                       "Additional Attribute Count",
                                       "Photos Captured Count",
                                       "Year Founded",
                                       "Verified",
                                       "Confirmed Verification Done",
                                       "Bucket",
                                       "Source",
                                       "Contact Person Position 2",
                                       "8 Digit SIC ( Source1 )",
                                       "SIC8 Category Name",
                                       "6 Digit SIC ( Source2 )",
                                       "SIC6 Category Name",
                                       "SIC ( Source3 )",
                                       "Credit Rating",
                                       "Additional Attributes",
                                       "Review Sources");

        ds = ds.withColumn("ReviewSourcesCount", size(split(col("Review Sources"), "Source:")).minus(1));

        ds = ds.withColumn("FaceBookLikesCount",
                           regexp_extract(col("Facebook Profile"), "(Likes:)(\\d{1,})", 2));
        ds = ds.withColumn("TwitterFollowersCount",
                           regexp_extract(col("Twitter Profile"), "(Followers:)(\\d{1,})", 2));
        ds = ds.withColumn("TwitterFollowingCount",
                           regexp_extract(col("Twitter Profile"), "(Following:)(\\d{1,})", 2));
        ds = ds.withColumn("TwitterTweetsCount",
                           regexp_extract(col("Twitter Profile"), "(Tweets:)(\\d{1,})", 2));

        ds = addPaymentMethodColumn(ds, "visa");
        ds = addPaymentMethodColumn(ds, "master");
        ds = addPaymentMethodColumn(ds, "cash");
        ds = addPaymentMethodColumn(ds, "debit");
        ds = addPaymentMethodColumn(ds, "check");
        ds = addPaymentMethodColumn(ds, "amex");
        ds = addPaymentMethodColumn(ds, "financing");

        ds = encodeOneHot(ds, "SIC Name / Category", "SICNameCategoryVector");
        ds = encodeOneHot(ds, "Category", "CategoryVector");
        ds = encodeOneHot(ds, "Full Category Set", "FullCategorySetVector");

        // TODO - move to tmp folder or delete
        // dump dataset to file for exploring
        String outPath = "hdfs://master/out/" + StringUtils.substringAfterLast(inFile, "/");

        log.info("dump file to {}", outPath);

        ds.write()
          .option("header", true)
          .option("quoteAll", true)
          .csv(outPath);

        log.info("Processing takes: {} ms", watch.getTime());

        return ds;
    }

    /**
     * Adds payment method support column with name 'AA_PM_support_$payMethod'
     *
     * Uses regex extract to retrieve payment method support from column 'Additional Attributes'
     *
     * @param ds        input datasource
     * @param payMethod payment method
     * @return output datasource with enriched column
     */
    private Dataset<Row> addPaymentMethodColumn(Dataset<Row> ds, String payMethod) {

        String colName = "AA_PM_support_" + payMethod;
        String regexTpl = "(Payment Methods:)[^\\|]{0,}(%s)";
        String preparedRegex = String.format(regexTpl, payMethod);

        return ds
            .withColumn(colName,
                        when(regexp_extract(col("Additional Attributes"), preparedRegex, 2)
                                 .equalTo(payMethod), lit(1))
                            .otherwise(lit(0)));
    }

    private void registerUdfs(SparkSession spark) {
        // register UDF for SparceVector to string encoding
        spark.udf().register(UDF_VECTOR_TO_STRING, (SparseVector s) -> udfVectorToSting(s), DataTypes.StringType);
    }

    /**
     * Encode column woth OneHotEncoder.
     *
     * Output column will be represented as SparseVector trancformed to String in format "n|idx|val".
     *
     * Where 'n' denoted number of elements in vector, 'idx' point to element wheve value exists, 'val' contains
     * value itself
     *
     * @param ds           - input datatser
     * @param inputColumn  - input column to encode
     * @param outputColumn - output column name
     * @return - output dataset
     */
    private Dataset<Row> encodeOneHot(final Dataset<Row> ds, final String inputColumn, final String outputColumn) {

        String tmpIndexColumn = "tmpCategoryIndex";
        String tmpVectorColumn = "tmpCategoryVector";

        StringIndexerModel indexer = new StringIndexer()
            .setInputCol(inputColumn)
            .setOutputCol(tmpIndexColumn)
            .setHandleInvalid("keep") // also "skip" ant "error" strategy is possible.
            .fit(ds);

        Dataset<Row> indexed = indexer.transform(ds);

        OneHotEncoder encoder = new OneHotEncoder().setInputCol(tmpIndexColumn).setOutputCol(tmpVectorColumn);
        Dataset<Row> res = encoder.transform(indexed);

        res = res.withColumn(outputColumn, callUDF(UDF_VECTOR_TO_STRING, col(tmpVectorColumn)));

        return res.drop(tmpIndexColumn, tmpVectorColumn);
    }

    /**
     * Translate {@link SparseVector} to String notation "n|idx|val"
     *
     * @return String representation of SparseVector
     */
    private static String udfVectorToSting(SparseVector vector) {
        Integer idx = vector.indices().length > 0 ? vector.indices()[0] : null;
        Double val = vector.values().length > 0 ? vector.values()[0] : null;
        return vector.size() + "|" + idx + "|" + val;
    }

}
