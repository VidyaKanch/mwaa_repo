MWAA Project
https://github.com/VidyaKanch/mwaa_repo.git



Project Title:   Designed and implemented an ETL pipeline using Amazon MWAA to automate data ingestion and transformation processes.



Description: 

	  Developed CI/CD pipeline using AWS Codepipeline which simplifies the deployment lifecycle by eliminating manual file uploads to AWS S3 and enabling seamless workflow migration from development to MWAA, with source control via GitHub.
	  
	  Developed Airflow DAG with tasks to:
	  
	  Downloads CSV data from external source
	  
	  Load raw data into a staging table
	  
	  Clean and transform data before inserting into a target table
	  
	  Configured AWS Secrets Manager as an alternative secrets backend to securely store Airflow connections and variables, replacing the default MWAA metadata store.

Planned Enhancements:

	  Testing and Validation: Extend the pipeline to incorporate data validation, testing, and quality checks as part of the workflow to align with organizational CI/CD and data quality standards.  
	  
	  Logging and Monitoring: Integrate Amazon CloudWatch Logs with Splunk via a subscription filter and AWS Lambda to support advanced monitoring, security analysis, and audit compliance.
	  
	  Cost Optimization: Implement an automated solution using Amazon EventBridge Scheduler and AWS Step Functions to periodically delete and recreate MWAA development environments, preserving metadata in S3 and reducing unnecessary runtime costs.
	  
	  Scalability for Resource-Intensive Jobs: For large jobs requiring additional resources, utilize the ECSOperator to run tasks on an ECS cluster instead of MWAA worker nodes. This approach mitigates the risk of out-of-memory or task timeout errors by offloading processing to a more scalable environment.



