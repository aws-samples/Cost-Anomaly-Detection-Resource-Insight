import boto3
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper(), logging.INFO))
#logging.getLogger().setLevel(logging.DEBUG)

def create_email_message(event):
    # Extract anomalies
    anomalies = event['detail']['anomalies']
    total_cost_increase = 0

    logger.debug(f"Total number of anomalies: {event['detail']['anomaly_count']}\n")
    logger.debug("Anomaly Details:")

    mod_email_message = " "
    total_cost_increase = 0.0
    for idx, anomaly in enumerate(anomalies, 1):
        message = f"""
        {idx}, {anomaly['line_item_usage_account_id']}, {anomaly['line_item_resource_id']}, {anomaly['product_servicename']}, ${round(float(anomaly['anomaly_period_cost']),2)}, ${round(float(anomaly['previous_period_cost']),2)}, ${round(float(anomaly['cost_increase']),2)}, {round(float(anomaly['percentage_increase']),2)}%"""
        total_cost_increase += float(anomaly['cost_increase'])
        mod_email_message = mod_email_message + message
        
    summary = f"The above anomalies caused a total cost increase of ${round(total_cost_increase,2)}"

    email_table = event['detail']['email_table']


    emailbody=f"""
    Hello,

    You are receiving this alert because AWS Cost Anomaly Detection has identified an unusual cost increase. 
    The anomaly has been validated and the root cause has been determined using the AWS Cost and Usage Report (CUR). 

    * Anomaly Start Date: {event['detail']['original_alert']['anomalyStartDate']}
    * Anomaly End Date: {event['detail']['original_alert']['anomalyEndDate']}

    Here is the list of the resources that triggered this cost anomaly:
    
    {email_table}
    

    Summary of the Alert: 
    
    {summary}
    

    Please verify if this cost increase is expected and, if necessary, make any adjustments.

    To view the original anomaly report, please click 
    {event['detail']['original_alert']['anomalyDetailsLink']}.

    Thank you,
    Anomaly Detection Agent 

    """
    logger.debug(f"emailbody is {emailbody}")
    return emailbody

def lambda_handler(event, context):

    logger.debug(event)
    emailSnsTopic = os.environ.get('SNS_TOPIC_ARN')
    emailSubject = 'AWS Cost Anomaly Detection Alert'
    emailMessage = create_email_message(event)
    
    if emailMessage is not None and emailSubject is not None and emailSnsTopic is not None:
        sns = boto3.client('sns')
        params = {
            'Message': emailMessage,
            'Subject': emailSubject,
            'TopicArn': emailSnsTopic
        }

        try:
            response = sns.publish(**params)
            logger.debug('MessageId: ' + response['MessageId'])
            return None

        except Exception as e:
            logger.error('Error publishing to SNS topic: ' + str(e))
            raise e
    else:
        logger.error('Skipping SNS publishing as emailMessage or emailSubject or emailSnsTopic is missing.')
        return None