export AWS_DEFAULT_PROFILE="spark"
export AWS_DEFAULT_REGION="us-west-2"

aws emr create-default-roles

aws emr create-cluster --name "Snowplow S3 to Redshift Historical Data Uploader Cluster" \
--profile spark \
--region us-west-2 \
--release-label emr-5.2.0 \
--applications Name=Spark \
--instance-type r3.8xlarge \
--instance-count 4 \
--configurations file://./snowplow-to-redshift-historical-data-uploader.json \
--log-uri s3://reevoosparklogs/  \
--enable-debugging \
--auto-terminate \
--use-default-roles \
--steps Type=Spark,Name="Snowplow S3 to Redshift Historical Data Uploader",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--class,com.reevoo.snowplow.SnowplowToRedshiftHistoricalDataUpload,s3://revoosparkapp/snowplow_spark_aggregators-assembly-1.0.jar,--fromDate,2016-06-10,--toDate,2016-06-10,--destinationTable,testing_table]


