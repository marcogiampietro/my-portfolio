import pandas as pd
import sqlalchemy as sa
import urllib
from dotenv import load_dotenv
import os
from datetime import datetime
from sqlalchemy.sql import text
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
load_dotenv()


def get_ambient(amb):
    if amb == "prod":
        serv = os.environ.get('PROD_SERVER_NAME')
        db = os.environ.get('PROD_DB_NAME')
        user = os.environ.get('PROD_USERNAME')
        pas = os.environ.get('PROD_PASSWORD')
    else:
        serv = os.environ.get('HML_SERVER_NAME')
        db = os.environ.get('HML_DB_NAME')
        user = os.environ.get('HML_USERNAME')
        pas = os.environ.get('HML_PASSWORD')
    return serv, db, user, pas


# -------------------------------------------AMBIENT-------------------------------------------------------
dirr: str = 'C:/Dados/'
ambient: str = "prod"
server_name, db_name, username, password = get_ambient(ambient)

# ---------------------------------------------BD CONNECTION-------------------------------------------------------
params = urllib.parse.quote_plus('Driver={SQL Server};'
                                 f'Server={server_name};'
                                 f'Database={db_name};'
                                 f'Uid={username};'
                                 f'Pwd={password}'
                                 )
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
conn = engine.connect()

# ---------------------------------------------PANDAS----------------------------------------------------------
query = conn.execute(text("select * from table1"))
df_report = pd.DataFrame(query.fetchall())

query_clients = conn.execute(text("SELECT * FROM table2"))
df_clients = pd.DataFrame(query_clients.fetchall())

# ---------------------------------------------DATE & SHAPE-----------------------------------------------------
extraction_datetime = datetime.now()
extraction_date = extraction_datetime.date().strftime('%d-%m-%Y')
extraction_time = extraction_datetime.time().strftime('%Hh%M')

(max_row, max_col) = df_report.shape
print("SHAPE", max_row, max_col)

# ----------------------------------------------EXCEL-------------------------------------------------------------
name_report = 'table1_' + extraction_date + ' ' + extraction_time + '.xlsx'
name_clients = 'table2_' + extraction_date + ' ' + extraction_time + '.xlsx'

path_report = dirr + name_report
path_clients = dirr + name_clients

pd.DataFrame({}).to_excel(path_report)
df_clients.to_excel(path_clients, index=False)

writer = pd.ExcelWriter(
    path_report,
    engine='xlsxwriter'
)

df_report.to_excel(
    writer,
    index=False,
    sheet_name='Data'
)

workbook = writer.book
worksheet1 = writer.sheets['Data']

columns_args = [
    {'header': 'column2'},
    {'header': 'column2'},
    {'header': '...'},
]

format_kwargs = {
    'autofilter': True,
    'header_row': True,
    'banded_rows': True,
    'columns': columns_args
}

worksheet1.add_table(
    first_row=0,
    first_col=0,
    last_row=max_row,
    last_col=(max_col - 1),
    options=format_kwargs
)

conn.close()
writer.close()

# -------------------------------------------------MAIL-------------------------------------------------------------

# SERVER CONFIGS
smtp_server = os.environ.get('SMTP_SERVER')
smtp_port = os.environ.get('SMTP_PORT')
smtp_username = os.environ.get('SMTP_USERNAME')
smtp_password = os.environ.get('SMTP_PASSWORD')

# MAIL CONFIGS
send_from = os.environ.get('SEND_FROM')
send_to = os.environ.get('SEND_TO').split(',')

subject = "[SUBJECT]"
body = f"""
[BODY]
"""

# E-MAIL'S MOUNT
msg = MIMEMultipart()
msg['From'] = send_from
msg['To'] = ', '.join(send_to)
msg['Subject'] = subject

msg.attach(MIMEText(body, 'plain'))

# ATTACHMENTS
with open(path_report, 'rb') as f:
    part = MIMEBase('application', "octet-stream")
    part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename=name_report)
    msg.attach(part)

with open(path_clients, 'rb') as f:
    part = MIMEBase('application', "octet-stream")
    part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename=name_clients)
    msg.attach(part)

# SENDER
smtp = smtplib.SMTP(smtp_server, int(smtp_port))
smtp.starttls()
smtp.login(smtp_username, smtp_password)
smtp.sendmail(send_from, send_to, msg.as_string())
smtp.quit()

print("Email Enviado")
