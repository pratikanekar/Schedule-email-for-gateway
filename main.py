from generate_excel_file_gw import analysis
from historic_generate_excel_file_gw import historic_analysis
from loguru import logger
from time import sleep
from email_service import send_mail

logger.add('/home/smartiam/PycharmProjects/Schedule-email-for-gateway/output.log', level="INFO")
if __name__ == "__main__":
    analysis()
    sleep(2)
    historic_analysis()
    sleep(5)
    send_mail()
