import os
import json
import logging
from datetime import datetime, timedelta
import boto3
import time
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper(), logging.INFO))
#logging.getLogger().setLevel(logging.DEBUG)

def lambda_handler(event, context):
    logger.debug(f"Incoming event: {json.dumps(event)}")
    
    if not event.get('Records'):
            logger.error("No Records found in event")
            return {
                'statusCode': 500,
                'body': 'No Records found in event'
            }
    
    for record in event['Records']:
        try:
            response, data = process_message_for_athena(record)
            logger.debug(f"reponse type {type(response)}")
            
            # Ensure response is a dictionary
            response_json = {
                "anomalies": response if isinstance(response, list) else [response],
                "anomaly_count": len(response) if isinstance(response, list) else 1
            }

            #response_json=json.loads(json.dumps(response))
            logger.debug(f"response_json type {type(response_json)}")

            table = format_data_as_table(data)
            email_table = {
                "email_table": table
            }
            response_json.update(email_table)
            original_alert = json.loads(f'{{ "original_alert": {record["Sns"]["Message"]} }}')
            logger.debug(f"original_alert type {type(original_alert)}")
            response_json.update(original_alert)
            logger.debug(f"json after merging: {json.dumps(response_json)}")
            eb_result = post_to_eventbridge(response_json)
            logger.debug(f"eb_result: {json.dumps(eb_result)}")
            
        except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
                logger.error(f"Failed record: {json.dumps(record)}")
                continue
    logger.debug("done")

    return {
        'statusCode': 200
    }

def post_to_eventbridge(event_detail):
    event_bus_source = os.environ.get('EVENT_BRIDGE_SOURCE_NAME')
    event_detail_type = os.environ.get('EVENT_BRIDGE_DETAIL_TYPE')
    event_bus_name = os.environ.get('EVENT_BRIDGE_BUS_NAME')
    if not event_bus_name:
        raise Exception("EVENT_BRIDGE_BUS_NAME environment variables not set.")
    
    if not event_detail_type:
        raise Exception("EVENT_BRIDGE_DETAIL_TYPE environment variables not set.") 
    if not event_bus_source:
        raise Exception("EVENT_BRIDGE_SOURCE_NAME environment variables not set.") 
    
    eventbridge = boto3.client('events')
    try:
        response = eventbridge.put_events(
            Entries=[
                {
                    'Source': event_bus_source,
                    'DetailType': event_detail_type,
                    'Detail': json.dumps(event_detail),
                    'EventBusName': event_bus_name  
                }
            ]
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Event published successfully',
                'eventID': response['Entries'][0]['EventId']
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing record: {str(e)}")
        raise

def process_message_for_athena(record):
    try:
        message = json.loads(record['Sns']['Message'])
        logger.info(f"Processed message {message}")
        
        # Create the filter condition
        conditions = []
        for cause in message['rootCauses']:
            logger.debug(f"rootCauses - 1 {cause}")
            condition = (f"line_item_usage_account_id = '{cause['linkedAccount']}' AND "
                    f"line_item_usage_type = '{cause['usageType']}' ")
            conditions.append(condition)
        logger.debug(f"conditions - 1 {conditions}")
        # Join all conditions with ' OR '
        account_service_and_usage_filter = ' OR '.join(conditions)

        # Wrap the entire output in parentheses
        account_service_and_usage_filter = f"({account_service_and_usage_filter})"

        # Parse start and end dates from the event
        start_date = datetime.strptime(message['anomalyStartDate'], "%Y-%m-%dT%H:%M:%SZ")
        end_date = datetime.strptime(message['anomalyEndDate'], "%Y-%m-%dT%H:%M:%SZ")

        logger.debug(f"start_date - {start_date} end_date {end_date}")
        # Calculate the duration of the anomaly
        duration = (end_date - start_date).days + 1

        # Calculate date parameters for the query
        query_start_date = start_date - timedelta(days=duration)
        query_end_date = end_date + timedelta(days=1)
        previous_period_start_date = start_date - timedelta(days=duration)
        previous_period_end_date = end_date - timedelta(days=duration)

        # Format the dates as strings for the SQL query
        query_start_date_str = query_start_date.strftime('%Y-%m-%d')
        query_end_date_str = query_end_date.strftime('%Y-%m-%d')
        previous_period_start_date_str = previous_period_start_date.strftime('%Y-%m-%d')
        previous_period_end_date_str = previous_period_end_date.strftime('%Y-%m-%d')
        current_period_start_date_str = start_date.strftime('%Y-%m-%d')
        current_period_end_date_str = end_date.strftime('%Y-%m-%d')

        # Specify the Athena table name
        table_name = os.environ.get('ATHENA_TABLE')
        if not table_name:
            raise Exception("ATHENA_TABLE environment variables not set.")
        #table_name = 'cur'

        athena_query = f"""
            WITH daily_costs AS (
                SELECT 
                    line_item_resource_id,
                    line_item_usage_account_id,
                    product_servicename,
                    DATE(line_item_usage_start_date) AS usage_date,
                    SUM(line_item_unblended_cost) AS total_cost
                FROM 
                    {table_name}
                WHERE 
                    {account_service_and_usage_filter}
                    AND line_item_usage_start_date >= DATE '{query_start_date_str}'
                    AND line_item_usage_start_date < DATE '{query_end_date_str}'
                GROUP BY 
                    line_item_resource_id, 
                    line_item_usage_account_id,
                    product_servicename,
                    DATE(line_item_usage_start_date)
            ),
            cost_summary AS (
                SELECT 
                    line_item_resource_id,
                    line_item_usage_account_id,
                    product_servicename,
                    SUM(CASE WHEN usage_date BETWEEN DATE '{current_period_start_date_str}' AND DATE '{current_period_end_date_str}' THEN total_cost ELSE 0 END) AS anomaly_period_cost,
                    SUM(CASE WHEN usage_date BETWEEN DATE '{previous_period_start_date_str}' AND DATE '{previous_period_end_date_str}' THEN total_cost ELSE 0 END) AS previous_period_cost
                FROM 
                    daily_costs
                GROUP BY 
                    line_item_resource_id,
                    line_item_usage_account_id,
                    product_servicename
            ),
            cost_growth AS (
            SELECT 
                    line_item_usage_account_id,
                    product_servicename,
                    line_item_resource_id,
                    anomaly_period_cost,
                    previous_period_cost,
                    (anomaly_period_cost - previous_period_cost) AS cost_increase,
                    CASE 
                        WHEN previous_period_cost = 0 THEN 100
                        ELSE ((anomaly_period_cost - previous_period_cost) / previous_period_cost) * 100
                    END AS percentage_increase
                FROM 
                    cost_summary
            )
            SELECT 
                line_item_usage_account_id,
                product_servicename,
                line_item_resource_id,
                anomaly_period_cost,
                previous_period_cost,
                cost_increase,
                percentage_increase
            FROM 
                cost_growth
            WHERE
                cost_increase > 0
            ORDER BY 
                cost_increase DESC
            LIMIT 5;
        """
        logger.debug(f"Generated Athena query {athena_query}")
        results, data = run_athena_query(athena_query)
        logger.debug(f"Athena results {json.dumps(results)}")    
        return results, data
    except Exception as e:
        logger.error(f"Error processing Athena message : {str(e)}")
        logger.error(traceback.format_exc())
        raise
    
def run_athena_query(query_id):
    """Return answer to Bedrock Agent in expected format."""
    try:
        # Extract parameters from the event
        database = os.environ.get('ATHENA_DATABSE')
        if not database:
            raise Exception("ATHENA_DATABSE environment variables not set.")
        ouput_s3_bucket = os.environ.get('ATHENA_OUTPUT_LOCATION')
        if not ouput_s3_bucket:
            raise Exception("ATHENA_OUTPUT_LOCATION environment variables not set.")
        
        output_location = f"s3://{ouput_s3_bucket}/"
        logger.debug(f'{query_id=}')
        
        # Initialize Athena client
        athena_client = boto3.client('athena')
        response = athena_client.start_query_execution(
            QueryString=query_id,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': output_location,
            }
        )
        query_execution_id = response['QueryExecutionId']
        
        # Wait for the query to complete
        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            
            time.sleep(1)
        
        if status == 'SUCCEEDED':
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            # Process the results as needed
            rows = results['ResultSet']['Rows']
            # First row contains column headers
            headers = [col['VarCharValue'] for col in rows[0]['Data']]
            
            # Process data rows
            data = []
            for row in rows[1:]:
                values = [field.get('VarCharValue', '') for field in row['Data']]
                row_dict = dict(zip(headers, values))
                data.append(row_dict)
            logger.debug(f"data results --> {data}")
            return data, rows
        else:
            return f"QUERY FAILED WITH STATUS ---- : {status}"
    except Exception as e:
        logger.error(traceback.format_exc())
        raise

def format_data_as_table(data):
    try:
        logger.debug(f"data in format_data_as_table {data}")
        # Extract headers and rows from the data
        headers = [item['VarCharValue'] for item in data[0]['Data']]
        rows = [[entry['VarCharValue'] for entry in row['Data']] for row in data[1:]]

        # Rename and order columns as required
        column_names = ["Account id", "Service", "Resource id", "Current Cost", "Previous Cost", "% Growth"]
        column_mapping = {
            "Account id": "line_item_usage_account_id",
            "Service": "product_servicename",
            "Resource id": "line_item_resource_id",
            "Current Cost": "anomaly_period_cost",
            "Previous Cost": "previous_period_cost",
            "% Growth": "percentage_increase",
        }

        mapped_rows = [
            [
                row[headers.index(column_mapping[column])]
                if column_mapping[column] in headers
                else ""
                for column in column_names
            ]
            for row in rows
        ]

        # Adjust column widths based on the data
        column_widths = [
            max(len(str(item)) for item in col)
            for col in zip(column_names, *mapped_rows)
        ]

        # Create the table separator
        separator = "-" * (sum(column_widths) + len(column_widths) * 3 + 1)

        # Format the header row
        header_row = "| " + " | ".join(
            column.ljust(width) for column, width in zip(column_names, column_widths)
        ) + " |"

        # Format the data rows
        data_rows = [
            "| " + " | ".join(
                str(cell).ljust(width) for cell, width in zip(row, column_widths)
            ) + " |"
            for row in mapped_rows
        ]

        # Combine everything into the final table
        table = "\n".join([separator, header_row, separator] + data_rows + [separator])
    except Exception as e:
        logger.error(traceback.format_exc())
        raise
    return table