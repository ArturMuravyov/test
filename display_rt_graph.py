import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import clear_output
import time
from pylab import rcParams
import os
from dbHandler import dbHandler

rcParams['figure.figsize'] = 12, 3

def display_rt_graph(conn):
    while True:
        try:
            db = dbHandler('sqlite3')             
            conn = db.connect()
            #read all the invoice data from the sqlite database 
            df = pd.read_sql_query("select InvoiceId,CustomerId,InvoiceDate,Total from invoice;", 
                                   conn, parse_dates=['InvoiceDate'], index_col='InvoiceDate')
            fig, axes = plt.subplots(ncols=2) #display two graphs to see the differences clearly
            sales = df.resample("M").sum() #sum of sales for one graph
            sales['Total'].plot(ax=axes[0])
            #unique customers for the other graph
            unique_customers = df['CustomerId'].resample('M').apply(lambda x: x.nunique())
            unique_customers.plot(ax=axes[1])
            fig.tight_layout()
            plt.pause(1)
            clear_output(wait=True)

        except Exception as e:
            pass

    
if __name__ == '__main__':
    try:
        db = dbHandler('sqlite3')             
        conn = db.connect()
        display_rt_graph(conn)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
