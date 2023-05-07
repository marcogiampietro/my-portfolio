from shareplum import Site
from shareplum import Office365
from configs import sharepoint_config,prod_BI_db
import pandas as pd
from sqlalchemy.sql import text
import sqlalchemy as sa
import urllib
import pyodbc

#CONFIG SHAREPOINT
url = 'https://XXXX.sharepoint.com/'
user = sharepoint_config.get('user')
psw = sharepoint_config.get('psw')
site = 'https://XXXX.sharepoint.com/sites/[XXXX]/'
list_name = '[list_name]'

#CONNECT SHAREPOINT
authcookie = Office365(url, username=user, password=psw).GetCookies()
site = Site(site, authcookie=authcookie)
sp_list = site.List(list_name)

#CONNECT PERMISSAO DATABASE
params = urllib.parse.quote_plus('Driver={SQL Server};'
                                 f'Server={prod_BI_db.get("server_name")};'
                                 f'Database={prod_BI_db.get("db_name")};'
                                 f'Uid={prod_BI_db.get("username")};'
                                 f'Pwd={prod_BI_db.get("password")}'
                                 )
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
conn = engine.connect()

sql = conn.execute(text("SELECT * FROM table"))
df = pd.DataFrame(sql.fetchall(),columns=['ID', 'Title', 'Guid'])

#LOOPING
for idx in df.index:
    query = {'Where': [('Eq', 'Guid', df.loc[idx,"Guid"])]}
    sp_items_filter = sp_list.GetListItems(query=query)
    
    if len(sp_items_filter) > 0:
        sp_items_filter[0]['ID)'] = df.loc[idx,"ID"]
        #UPDATE SHAREPOINT
        sp_list.update_list_items(data=sp_items_filter, kind='Update')
    else:
        registro = {
            'ID': df.loc[idx,"ID"],
            'Title': df.loc[idx,"Title"],
            'Guid':df.loc[idx,"Guid"]
        }
        print(df.loc[idx,"Guid"])
        sp_list.update_list_items(data=[registro], kind='New')
        
