import boto3
import logging
import os
from botocore.config import Config

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper(), logging.INFO))

# Configure exponential backoff
retry_config = Config(
    retries={
        'max_attempts': 5,
        'mode': 'adaptive'
    }
)

def create_email_content(event):
    """
    Create HTML and text email content from CADRI anomaly event
    """
    try:
        logger.debug(f"Processing event with {len(event.get('detail', {}).get('anomalies', []))} anomalies")
        anomalies = event['detail']['anomalies']
        anomaly_count = event['detail']['anomaly_count']
        original_alert = event['detail']['original_alert']
        
        # Calculate total cost increase
        total_cost_increase = sum(float(anomaly['cost_increase']) for anomaly in anomalies)
        logger.debug(f"Total cost increase calculated: ${total_cost_increase}")
        
        # Create HTML table rows
        html_rows = ""
        text_rows = ""
        
        for anomaly in anomalies:
            html_rows += f"""
                <tr>
                    <td>{anomaly['line_item_usage_account_id']}</td>
                    <td>{anomaly['product_servicename']}</td>
                    <td style="word-break: break-all;">{anomaly['line_item_resource_id']}</td>
                    <td>${round(float(anomaly['anomaly_period_cost']), 2)}</td>
                    <td>${round(float(anomaly['previous_period_cost']), 2)}</td>
                    <td>${round(float(anomaly['cost_increase']), 2)}</td>
                    <td>{round(float(anomaly['percentage_increase']), 2)}%</td>
                </tr>
            """
            
            text_rows += f"""
    {anomaly['line_item_usage_account_id']}\t{anomaly['product_servicename']}\t{anomaly['line_item_resource_id']}\t${round(float(anomaly['anomaly_period_cost']), 2)}\t${round(float(anomaly['previous_period_cost']), 2)}\t${round(float(anomaly['cost_increase']), 2)}\t{round(float(anomaly['percentage_increase']), 2)}%"""
        
        # Get dates and link
        anomaly_start_date = original_alert.get('anomalyStartDate', 'UNAVAILABLE')
        anomaly_end_date = original_alert.get('anomalyEndDate', 'UNAVAILABLE')
        anomaly_link = original_alert.get('anomalyDetailsLink', '')
        
        body_html = f"""
        <html>
        <head>
            <style>
                table {{
                    border: 1px solid #ddd;
                    padding: 0;
                    margin: 0;
                    font-size: 1em;
                    width: 100%;
                    border-collapse: collapse;
                }}

                table th,
                table td {{
                    padding: 10px;
                    background: #fcfcfc;
                    text-align: center;
                    vertical-align: middle;
                    border: 1px solid #ddd;
                }}

                table tr:nth-child(even) td {{
                    background: #f2f2f2;
                }}

                table td {{
                    font-size: .85em;
                }}

                table thead tr th {{
                    font-size: 1em;
                    font-weight: 700;
                    background-color: #f79d2e;
                    color: #131212;
                }}
            </style>
        </head>
        <body>
            <p>Hello,</p>
            <p>You are receiving this alert because AWS Cost Anomaly Detection has identified an unusual cost increase. 
            The anomaly has been validated and the root cause has been determined using the AWS Cost and Usage Report (CUR).</p>
            
            <p><strong>Anomaly Details:</strong></p>
            <ul>
                <li>Anomaly Start Date: {anomaly_start_date}</li>
                <li>Anomaly End Date: {anomaly_end_date}</li>
                <li>Total Anomalies: {anomaly_count}</li>
                <li>Total Cost Increase: ${round(total_cost_increase, 2)}</li>
            </ul>
            
            <h3>Resources that triggered this cost anomaly:</h3>
            <table>
                <thead>
                    <tr>
                        <th>Account ID</th>
                        <th>Service</th>
                        <th>Resource ID</th>
                        <th>Current Cost</th>
                        <th>Previous Cost</th>
                        <th>Cost Increase</th>
                        <th>% Increase</th>
                    </tr>
                </thead>
                <tbody>
                    {html_rows}
                </tbody>
            </table>
            
            <p>Please verify if this cost increase is expected and, if necessary, make any adjustments.</p>
            
            <p>To view the original anomaly report, please <a href="{anomaly_link}">click here</a>.</p>
            
            <p>Thank you,<br>
            Anomaly Detection Agent</p>
        </body>
        </html>
        """
        
        body_text = f"""
        Hello,
        
        You are receiving this alert because AWS Cost Anomaly Detection has identified an unusual cost increase.
        The anomaly has been validated and the root cause has been determined using the AWS Cost and Usage Report (CUR).
        
        Anomaly Details:
        - Anomaly Start Date: {anomaly_start_date}
        - Anomaly End Date: {anomaly_end_date}
        - Total Anomalies: {anomaly_count}
        - Total Cost Increase: ${round(total_cost_increase, 2)}
        
        Resources that triggered this cost anomaly:
        Account ID\tService\tResource ID\tCurrent Cost\tPrevious Cost\tCost Increase\t% Increase{text_rows}
        
        Please verify if this cost increase is expected and, if necessary, make any adjustments.
        
        To view the original anomaly report, please visit: {anomaly_link}
        
        Thank you,
        Anomaly Detection Agent
        """
        
        logger.debug("Email content created successfully")
        return body_html, body_text
        
    except Exception as e:
        logger.error(f'Error creating email content: {str(e)}')
        raise e

def get_verified_emails(ses_client, email_list):
    """
    Check which emails are verified in SES
    """
    logger.debug(f"Checking verification status for emails: {email_list}")
    verified = []
    for email in email_list:
        try:
            response = ses_client.get_identity_verification_attributes(Identities=[email])
            if response['VerificationAttributes'].get(email, {}).get('VerificationStatus') == 'Success':
                verified.append(email)
                logger.debug(f"Email {email} is verified")
            else:
                logger.debug(f"Email {email} is not verified")
        except Exception as e:
            logger.debug(f"Error checking verification for {email}: {str(e)}")
    logger.debug(f"Verified emails: {verified}")
    return verified

def modify_email_content(body_html, body_text, unverified_emails, fallback_email):
    """
    Add notice about unverified emails to content
    """
    
    unverified_notice_html = f"""
    <div style="margin: 20px 0; padding: 10px; background-color: #fff3cd; border: 1px solid #ffeeba; border-radius: 4px;">
        <p><strong>Note:</strong> This email was sent to {fallback_email} because the following email address is not verified in AWS SES:</p>
        <ul>
            {''.join(f'<li>{email}</li>' for email in unverified_emails)}
        </ul>
        <p>To receive these notifications directly, please contact your AWS administrator to verify these email addresses.</p>
    </div>
    """
    modified_html = body_html.replace('<body>', f'<body>{unverified_notice_html}')
    
    text_notice = f"Note: This email was intended for {', '.join(unverified_emails)} but was sent to {fallback_email} because the original recipient(s) are not verified in SES.\n\n"
    modified_text = body_text.replace('Hello,', f'Hello,\n\n{text_notice}')
    
    return modified_html, modified_text

def lambda_handler(event, context):
    """
    Main Lambda handler for sending CADRI cost anomaly alerts via SES
    """
    try:
        # Get environment variables
        sender_email = os.environ.get('SENDER_EMAIL')
        recipient_emails = os.environ.get('RECIPIENT_EMAIL')
        
        if not sender_email or not recipient_emails:
            raise Exception("SENDER_EMAIL and RECIPIENT_EMAIL environment variables must be set")
        
        # Parse comma-separated emails
        email_list = [email.strip() for email in recipient_emails.split(',')]
        logger.debug(f"Parsed recipient emails: {email_list}")
        
        # Initialize SES client
        ses = boto3.client('ses', config=retry_config)
        logger.debug("SES client initialized")
        
        # Check verified emails
        verified_emails = get_verified_emails(ses, email_list)
        unverified_emails = [email for email in email_list if email not in verified_emails]
        logger.info(f"Email verification results - Verified: {len(verified_emails)}, Unverified: {len(unverified_emails)}")
        
        body_html, body_text = create_email_content(event)
        responses = []
        logger.debug("Starting email sending process")
        
        # Send to verified recipients
        if verified_emails:
            response = ses.send_email(
                Source=sender_email,
                Destination={'ToAddresses': verified_emails},
                Message={
                    'Subject': {'Charset': 'UTF-8', 'Data': 'AWS Cost Anomaly Detection Resource Insight Alert'},
                    'Body': {
                        'Html': {'Charset': 'UTF-8', 'Data': body_html},
                        'Text': {'Charset': 'UTF-8', 'Data': body_text},
                    },
                },
            )
            responses.append(response['MessageId'])
            logger.info(f"Email sent to verified recipients {verified_emails}. MessageId: {response['MessageId']}")
        
        # Send notification to sender if there are unverified emails
        if unverified_emails:
            modified_html, modified_text = modify_email_content(body_html, body_text, unverified_emails, sender_email)
            response = ses.send_email(
                Source=sender_email,
                Destination={'ToAddresses': [sender_email]},
                Message={
                    'Subject': {'Charset': 'UTF-8', 'Data': 'AWS Cost Anomaly Detection Resource Insight Alert - Unverified Recipients'},
                    'Body': {
                        'Html': {'Charset': 'UTF-8', 'Data': modified_html},
                        'Text': {'Charset': 'UTF-8', 'Data': modified_text},
                    },
                },
            )
            responses.append(response['MessageId'])
            logger.info(f"Notification sent to sender about unverified emails {unverified_emails}. MessageId: {response['MessageId']}")
        
        if not verified_emails and not unverified_emails:
            logger.error("No recipients found in email list")
            raise Exception("No recipients found")
        
        logger.info(f"Email sending completed successfully. Total emails sent: {len(responses)}")
        
        return {
            'statusCode': 200,
            'body': f'Successfully sent emails. MessageIds: {responses}'
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error sending email alert: {str(e)}'
        }