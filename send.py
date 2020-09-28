import pika
import os

def send():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='upload files')

    directory = os.getcwd()
    extensions = ('.csv', '.json') # the tuple of legal file types

    for file in os.listdir():
        if file.endswith(extensions): 
            full_path = os.path.join(directory, file)
            if os.path.isfile(full_path):
                print (full_path)
                file_type = full_path.split(".")[-1]
                #since it's the only table in the assignment and there is no clear way of extracting 
                #the table name from the files i just make it "hard-coded" as invoice"
                table_name = 'invoice'
                message = 'path:' + full_path + ', file_type:' + file_type + ', table_name:' + table_name
                print(message)
                channel.basic_publish(exchange='', routing_key='upload files', body=message)

    connection.close()
    
if __name__ == '__main__':
    send()