import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS", notification_id)
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    conn_string = os.environ['DB_URL']
    conn = psycopg2.connect(conn_string)
    print("Connection established")

    cursor = conn.cursor()

    try:
        # TODO: Get notification message and subject from database using the notification_id
        notification_message = cursor.execute("SELECT message FROM Notification WHERE id=notification_id;")
        notification_subject = cursor.execute("SELECT subject FROM Notification WHERE id=notification_id;")


        # TODO: Get attendees email and name
        cursor.execute("SELECT first_name, last_name, email FROM Attendee;")
        attendees = cursor.fetchall()


        # TODO: Loop through each attendee and send an email with a personalized subject
        #attendees = Attendee.query.all()
        for attendee in attendees:
            subject = '{}: {}'.format(attendee.first_name, notification.subject)
            send_email(attendee.email, subject, notification_message)

            
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        notification.completed_date = datetime.utcnow()
        notification.status = 'Notified {} attendees'.format(len(attendees))
        #db.session.commit()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        conn.rollback()
    finally:
        # TODO: Close connection
        cursor.close()
        conn.close()

