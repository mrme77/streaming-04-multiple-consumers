 """
This program sends a message to a queue on the RabbitMQ server.
Make tasks harder/longer-running by adding dots at the end of the message.
Press CTRL+C to peacefully exit the program.

Author: Pasquale Salomone
Date: September 6, 2023
"""

import pika
import sys
import webbrowser
import logging
import time
import csv
# RabbitMQ configuration
rabbit_host = 'localhost'
queue_name = 'task_queue2'

# CSV file path
csv_file = 'Airports2.csv'  

# Variable to control whether to offer to open RabbitMQ Admin site
show_offer = True

def offer_rabbitmq_admin_site():
    """Open the RabbitMQ Admin website without asking"""
    global show_offer
    if show_offer:
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    '''This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.
    Press CTRL+C to peacefully exit the program.

    Args:
        host (str): The RabbitMQ server host.
        queue_name (str): The name of the RabbitMQ queue.
        message (str): The message to send.
   '''
    try:
        # Construct the log file name based on the queue name
        log_file = f"{queue_name}_file.log"
        # Configure the logging settings
        logging.basicConfig(filename=log_file, level=logging.INFO)

        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # log the message instead of printing it
        dot_count = message.count('.')
        time.sleep(dot_count)
        logging.info(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    
    
    finally:
        # close the connection to the server
        conn.close()

def main(csv_file_path: str):
    # Open RabbitMQ Admin website
    offer_rabbitmq_admin_site()

    # Read tasks from the specified CSV file
    with open(csv_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader) # skip the header row
        try:
           for row in reader:
               message = ','.join(row)  # Convert row to a comma-separated string
               message = message+".."
               send_message(rabbit_host, queue_name, message)
        except KeyboardInterrupt:
                print()
                print(" [x] Peacefully exiting application with CTRL+C")
                sys.exit(0)
    

if __name__ == "__main__":
    # Specify the CSV file name as a command-line argument
    print(" [*] Ready for work. To exit press CTRL+C")
    try:
        if len(sys.argv) != 2:
            print("Usage: python3 v3_emitter_of_tasks.py <csv_file>")
            sys.exit(1)

        csv_file = sys.argv[1]
        main(csv_file)
    except KeyboardInterrupt:
        print()
        print(" User interrupted process with CTRL-C keyboard shortcut.")
        sys.exit(0)
