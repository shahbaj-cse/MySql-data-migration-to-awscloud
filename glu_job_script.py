import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer_raw
Customer_raw_node1739532348082 = glueContext.create_dynamic_frame.from_catalog(database="retail_db", table_name="customers", transformation_ctx="Customer_raw_node1739532348082")

# Script generated for node orders_raw
orders_raw_node1739532412747 = glueContext.create_dynamic_frame.from_catalog(database="retail_db", table_name="orders", transformation_ctx="orders_raw_node1739532412747")

# Script generated for node transaction_raw
transaction_raw_node1739532414728 = glueContext.create_dynamic_frame.from_catalog(database="retail_db", table_name="transactions", transformation_ctx="transaction_raw_node1739532414728")

# Script generated for node final_customers
final_customers_node1739623084877 = ApplyMapping.apply(frame=Customer_raw_node1739532348082, mappings=[("address", "string", "address", "string"), ("phone", "string", "phone", "string"), ("name", "string", "name", "string"), ("created_at", "timestamp", "created_at", "timestamp"), ("customer_id", "int", "customer_id", "int"), ("email", "string", "email", "string")], transformation_ctx="final_customers_node1739623084877")

# Script generated for node final_orders
final_orders_node1739622558903 = ApplyMapping.apply(frame=orders_raw_node1739532412747, mappings=[("order_date", "timestamp", "order_date", "timestamp"), ("quantity", "int", "quantity", "int"), ("total_price", "decimal", "total_price", "decimal"), ("product_id", "int", "product_id", "int"), ("customer_id", "int", "customer_id", "int"), ("order_id", "int", "order_id", "int")], transformation_ctx="final_orders_node1739622558903")

# Script generated for node final_transactions
final_transactions_node1739622617375 = ApplyMapping.apply(frame=transaction_raw_node1739532414728, mappings=[("transaction_id", "int", "transaction_id", "int"), ("transaction_date", "timestamp", "transaction_date", "timestamp"), ("amount", "decimal", "amount", "decimal"), ("customer_id", "int", "customer_id", "int"), ("payment_method", "string", "payment_method", "string"), ("status", "string", "status", "string")], transformation_ctx="final_transactions_node1739622617375")

# Script generated for node customers_load_to_redshift
customers_load_to_redshift_node1739622717449 = glueContext.write_dynamic_frame.from_catalog(frame=final_customers_node1739623084877, database="redshift_retail_db", table_name="dev_public_customers", redshift_tmp_dir="s3://temp-data-inmigration",additional_options={"aws_iam_role": "arn:aws:iam::476114141463:role/redshift_role"}, transformation_ctx="customers_load_to_redshift_node1739622717449")

# Script generated for node orders_load_to_redshift
orders_load_to_redshift_node1739622729268 = glueContext.write_dynamic_frame.from_catalog(frame=final_orders_node1739622558903, database="redshift_retail_db", table_name="dev_public_orders", redshift_tmp_dir="s3://temp-data-inmigration",additional_options={"aws_iam_role": "arn:aws:iam::476114141463:role/redshift_role"}, transformation_ctx="orders_load_to_redshift_node1739622729268")

# Script generated for node trans_load_to_redshift
trans_load_to_redshift_node1739622735083 = glueContext.write_dynamic_frame.from_catalog(frame=final_transactions_node1739622617375, database="redshift_retail_db", table_name="dev_public_transactions", redshift_tmp_dir="s3://temp-data-inmigration",additional_options={"aws_iam_role": "arn:aws:iam::476114141463:role/redshift_role"}, transformation_ctx="trans_load_to_redshift_node1739622735083")

job.commit()
    print("Error sending SNS Notification:", str(e))
