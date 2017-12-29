package com.localblox.ashfaq.action;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_extract;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.when;

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
import org.apache.commons.lang3.ArrayUtils;
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Action for processing input files with AWS.
 *
 * Access to AWS should be configured through setting enviromnemt variables:
 *
 * export AWS_ACCESS_KEY_ID=my.aws.key
 *
 * export AWS_SECRET_ACCESS_KEY=my.secret.key
 *
 * http://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3
 *
 */
public class NewFileInFolderActionAWSImpl extends NewFileInFolderAction {

    private static final Logger log = LoggerFactory.getLogger(NewFileInFolderActionAWSImpl.class);

    private static final String STATUS_COMPLETE = "COMPLETED";
    private static final String BATCH_PREDICTION_NAME = "EXAMPLE";
    private static final String ML_MODEL_ID = "EXAMPLE-pr-2014-09-12-15-14-04-924";

    // TODO get from AppConfig
    private static final String S3_OUTPUT_PATTERN = "s3://eml-test-EXAMPLE/test-outputs/%s/results";

    // TODO get from AppConfig
    private static final String S3_DATA_LOCATION = "s3://people-match-ai-input-data/train/1/";

    // TODO get from AppConfig
    private static final String S3_DATA_LOCATION_SCHEMA = "s3://people-match-ai-input-data/train/1/data.csv.schema";

    private static final String DATASOURCE_ID = "exampleDataSourceId";
    private static final String DATASOURCE_NAME = "exampleDataSourceName";

    private static final String UDF_VECTOR_TO_STRING = "udfVectorToSting";

    private static final String[] DESIRED_COLUMNS = new String[]{
        "LocalBlox ID", "Address", "City Name",
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
        "HQ_Ticker", "HQ_Exchange", "HQ_Acquisitions",
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
        "Review Sources",

        // added 27.12.2017
        "SalesRange Normalized",
        "EmployeeRange Normalized",
        "HQ SalesRange Normalized",
        "HQ EmployeeRange Normalized",
        "Price Range Normalized",
        "Wi Fi Normalized",
        "Cuisine Normalized",
        "Parking Normalized",
        "Verified Normalized",
        "Ambience Normalized",
        "Location Type Normalized",
        "State of Incorporation Normalized",
        "Tags Normalized",
        "By Appointment Only Normalized",
        "Yelp Reviews Count Normalized",
        "Yelp Photos Count Normalized",
        "Foursquare Average Review Rating Normalized",
        "Foursquare Reviews Count Normalized",
        "Foursquare Photos Count Normalized",
        "Foursquare Visitors Normalized",
        "Foursquare Checkins Normalized",
        "Foursquare Tips Normalized",
        "Facebook Reviews Count Normalized",
        "Facebook Photos Count Normalized",
        "Facebook Visitors Normalized",
        "GooglePlaces Reviews Count Normalized",
        "GooglePlaces Photos Count Normalized",
        "CitySearch Reviews Count Normalized",
        "CitySearch Photos Count Normalized",
        "Full Category Set Normalized",
        "User Count Normalized",
        "Number of Votes on FourSquare Normalized",
        "# of PeopleTalking on Facebook",
        "# of Likes on Facebook",
        "Tweets Count",
        "Followers Count",
        "Following Count",
        "Average Review Rating on Google Normalized",
        "Average Review Rating on CitySearch Normalized",
        "CitySearch Link",
        "Facebook Link",
        "FourSquare Link",
        "GooglePlaces Link",
        "Manta Link",
        "Twitter Link",
        "Yelp Link",

        // target column for m-learning
        "sales"
    };

    @Override
    public void doIt(final String inFile) throws RuntimeException {
        // 1. Read file from HDFS IN folder
        SparkSession spark = getSparkSession();
        Dataset<Row> selectedData = readFromHDFS(spark, inFile);
        // 3. Load input file to AWS datasource
        loadDataToS3(selectedData);

        // init amazon machine learning client
        AmazonMachineLearning amazonMLClient = getAmazonMLClient();
        // 2. Create DataSource (name from filename)
        GetDataSourceResult dataSource = createDataSource(amazonMLClient);
        // 4. Create Batch prediction with params (DataSource ID, ...)
        GetBatchPredictionResult batchPredictionResult = createBatchPrediction(amazonMLClient, dataSource, inFile);

        // TODO 6. Read Prediction result from S3
        // TODO 7. write result into OUT folder in HDFS
    }

    /**
     * Init Amazon ML client for US_EAST_1 region.
     *
     * Credentials configured through environment variables
     *
     */
    private AmazonMachineLearning getAmazonMLClient() {
        return AmazonMachineLearningClientBuilder
            .standard()
//            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
//                AppConfig.getInstance().getS3AccessKeyId(),
//                AppConfig.getInstance().getS3SecretAccessKey())))
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

        log.info("Starting to load data to S3: {}", S3_DATA_LOCATION);

        // FIXME - there are still access denied problem to AWS.
        selectedData.write().csv(S3_DATA_LOCATION);

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
                               .option("escape", "\"")
                               .csv(inFile);

        String[] existingDesiredColumns = getExistingDesiredColumns(ds.columns(), DESIRED_COLUMNS);

        log.info("[{}] columns to be selected from dataset: {}", existingDesiredColumns.length,
                 Arrays.toString(existingDesiredColumns));

        String firstCol = existingDesiredColumns[0];
        existingDesiredColumns = ArrayUtils.remove(existingDesiredColumns, 0);

        ds = ds.select(firstCol, existingDesiredColumns);

        ds = transformColumnAware(ds,
                                  "Review Sources",
                                  (dataSet, colName) -> dataSet
                                      .withColumn("ReviewSourcesCount",
                                                  size(split(col(colName), "Source:")).minus(1)));

        ds = transformColumnAware(ds,
                                  "Facebook Profile",
                                  (dataSet, colName) -> dataSet
                                      .withColumn("FaceBookLikesCount",
                                                  regexp_extract(col(colName), "(Likes:)(\\d{1,})", 2)));

        ds = transformColumnAware(ds,
                                  "Twitter Profile",
                                  (dataSet, colName) -> dataSet
                                      .withColumn("TwitterFollowersCount",
                                                  regexp_extract(col(colName), "(Followers:)(\\d{1,})", 2)));

        ds = transformColumnAware(ds,
                                  "Twitter Profile",
                                  (dataSet, colName) -> dataSet
                                      .withColumn("TwitterFollowingCount",
                                                  regexp_extract(col(colName), "(Following:)(\\d{1,})", 2)));

        ds = transformColumnAware(ds,
                                  "Twitter Profile",
                                  (dataSet, colName) -> dataSet
                                      .withColumn("TwitterTweetsCount",
                                                  regexp_extract(col(colName), "(Tweets:)(\\d{1,})", 2)));

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

        ds.coalesce(1)
          .write()
          .option("header", true)
          .option("escape", "\"")
          .option("quoteAll", true)
          .csv(outPath);

        log.info("Processing takes: {} ms", watch.getTime());

        return ds;
    }

    /**
     * transforms colim if exists in dataset
     *
     * @param ds      input datatset
     * @param colName column name to transform
     * @param func    transformation logic
     * @return modified dataset or old one if column does not exist
     */
    private Dataset<Row> transformColumnAware(Dataset<Row> ds,
                                              String colName,
                                              BiFunction<Dataset<Row>, String, Dataset<Row>> func) {

        if (containsColumn(ds, colName)) {
            return func.apply(ds, colName);
        } else {
            log.warn("skip column transformation {} as it does not exist in dataset", colName);
            return ds;
        }

    }

    /**
     * Get existing desired column names as intersection with real column and desired column array/
     * @param existing existing columns from dataset
     * @param desired desired columns
     * @return column intersection
     */
    public static String[] getExistingDesiredColumns(String[] existing, String[] desired) {
        Set<String> s1 = new HashSet<>(Arrays.asList(existing));
        Set<String> s2 = new HashSet<>(Arrays.asList(desired));
        s1.retainAll(s2);

        return s1.toArray(new String[s1.size()]);
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
        String sourceColumn = "Additional Attributes";
        String regexTpl = "(Payment Methods:)[^\\|]{0,}(%s)";
        String preparedRegex = String.format(regexTpl, payMethod);

        if (containsColumn(ds, sourceColumn)) {
            return ds
                .withColumn(colName,
                            when(regexp_extract(col(sourceColumn), preparedRegex, 2)
                                     .equalTo(payMethod), lit(1))
                                .otherwise(lit(0)));
        } else {
            return ds;
        }

    }

    private boolean containsColumn(final Dataset<Row> ds, final String colName) {
        return ArrayUtils.contains(ds.columns(), colName);
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

        if (containsColumn(ds, inputColumn)) {

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
        } else {
            return ds;
        }

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
