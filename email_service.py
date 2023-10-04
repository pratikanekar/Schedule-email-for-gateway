import smtplib
import ssl
from datetime import datetime
from loguru import logger
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


def send_mail():
    logger.info(f"Email writing work in progress..!")
    gmail_user = 'pratik.a@smart-iam.com'
    gmail_password = 'Kiam@123456'

    # Recipient email address
    # to_email = 'manjusha.d@smart-iam.com'
    recipients = ['dheeraj.d@smart-iam.com', 'manjusha.d@smart-iam.com', 'techsupport1@smart-iam.com', 'hitesh.c@smartiam.in', 'punecms@smartiam.in']

    # Create the email message
    subject = 'All Gateway Analysis Data'
    message_body = 'Respected Sir/Madam, \n\nThis mail for testing purpose please ignore this mail..... \n\nI hope this email finds you well. I am writing to inform you that I have successfully completed the analysis of the Gateway data, as requested. I have attached the Excel file containing the results of the analysis to this email for your review and reference. \n\nThis is system generated mail please do not replay..! \n\nThanks and Regards \nPratik Anekar'
    message = MIMEMultipart()
    message['From'] = gmail_user
    message['To'] = ', '.join(recipients)  # Join recipients with a comma and space
    # message['To'] = to_email
    message['Subject'] = subject
    message.attach(MIMEText(message_body, 'plain'))

    # Attach an Excel file (change the filename and path to your file)
    now = datetime.now().strftime("%Y-%m-%d")
    file_path = f"/home/smartiam/PycharmProjects/Schedule-email-for-gateway/download/gw_analysis_info_{now}.xlsx"
    with open(file_path, 'rb') as attachment:
        part = MIMEApplication(attachment.read(), Name=f'gw_analysis_info_{now}.xlsx')

    # Add the Excel file as an attachment
    part['Content-Disposition'] = f'attachment; filename="gw_analysis_info_{now}.xlsx"'
    message.attach(part)

    try:
        context = ssl.create_default_context()
        # Connect to Gmail's SMTP server
        server = smtplib.SMTP_SSL('shared40.accountservergroup.com', 465, context=context)
        # server.starttls()  # Start TLS encryption

        # Log in to your Gmail account
        server.login(gmail_user, gmail_password)

        # Send the email
        server.sendmail(gmail_user, recipients, message.as_string())
        logger.info('Email sent successfully')

        # Quit the server
        server.quit()

    except Exception as e:
        logger.error('Error:', str(e))
