import pika, sys, os
import pandas
import sqlite3
from dbHandler import dbHandler
    

def receive(conn):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='upload files')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        #split the message received to file_path, file_type, table_name
        file_path = str(body).split('path:')[1].split(',')[0]
        file_type = str(body).split('file_type:')[1].split(',')[0]
        table_name = str(body).split('table_name:')[1].split("'")[0]
        #read json or csv accorsingly to file type
        if file_type == 'csv':
            df = pandas.read_csv(file_path)
        elif file_type == 'json':
            df = pandas.read_json(file_path)
        #upload the DataFrame to the sqlite database
        df.to_sql(table_name, conn, if_exists='append', index=False)

        
    channel.basic_consume(queue='upload files', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        db = dbHandler('sqlite3')             
        conn = db.connect()
        receive(conn)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)