# Prerequisites

1. Configuring CUR with AWS Data Export

    1. Sign in to the [AWS Management console](https://aws.amazon.com/console/)
    2. Navigate to **AWS Billing and  Cost Management**. Select **Data Export** and click on **Create** to begin setting up your export.
    3. Choose **Standard  data export**,  provide your export a name and for select **CUR 2.0** as the data table type.
    4. Enabling **Include resource IDs** and **Split cost allocation data** are optional
    5. Select Time granularity as **Hourly**
    6. Set **Parquet** as the compression format, and select the **Overwrite existing data export file** for file versioning.
    7. Specify the destination Amazon S3 bucket and a path prefix where CUR 2.0 data should be stored.
    8. Complete the setup by selecting **Create**.

2. Configuring AWS Glue to query CUR Data

    1. Navigate to **AWS Glue** console and select **Data Catalog** > **Crawlers**  to initiate the process of cataloging the CUR 2.0 data.
    2. Click on **Create Crawler**  and assign a unique crawler name.
    3. For the question **Is your data already mapped to Glue tables?** select **Not yet**
    4. Click **Add a data source**, select **S3** and specify the Amazon S3 location  from Step 1.8, where your CUR 2.0 data is exported, using the format: `s3://<bucket-name>/<prefix>/<export-name>/data/`. 
    5. Click **Add an S3 data source** and then click **Next**
    6. Click on **Create new IAM** role which will create the new AWS Glue role on your behalf. This role allows Glue to access the S3 bucket where CUR2.0 files are stored.
    7. Create a target database by clicking **Add database**. Provide a database name and click **Create database**.
    8. Navigate back to the AWS Glue console and select the database created in the previous step. Set the crawler schedule to **On demand** to run only when required.
    9. Confirm your settings and select **Create  Crawler**.
    10. Once the crawler is ready, select it and click **Run**. This will process and catalog the data, creating tables accessible by Amazon Athena.