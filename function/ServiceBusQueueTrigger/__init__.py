import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

SENDGRID_API_KEY = "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"
def main(msg: func.ServiceBusMessage):
    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    conn = psycopg2.connect(host='sonny-postgres-server.postgres.database.azure.com', 
                            user='sonnyadmin@sonny-postgres-server',
                            password='Thanhcong123###', 
                            dbname='techconfdb', port=5432)
    
    try:
        curs = conn.cursor()
        # TODO: Get notification message and subject from database using the notification_id
        notification = getNotificationById(notification_id, curs)
        logging.info(notification)
        # TODO: Get attendees email and name
        attendees = getAttendees(curs)
        logging.info(attendees)
        # TODO: Loop through each attendee and send an email with a personalized subject
        successNotification = 0
        for attendee in attendees:
            emailTemp = generateEmailTemp(attendee, notification)
            try:
                sendingClient = SendGridAPIClient(api_key=os.environ.get(SENDGRID_API_KEY))
                sendingClient.send(emailTemp)
                successNotification += 1
            except Exception as e:
                print (e)
        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        updateNotification(notification_id, successNotification, curs, conn)

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        if conn:
            curs.close()
            conn.close()

def getNotificationById(id: str, cursor: any):
    cursor.execute(f"select message, subject from notification where id='{id}';")
    return cursor.fetchall()

def getAttendees(cursor: any):
    cursor.execute(f"select first_name, last_name, email from attendee;")
    return cursor.fetchall()

def generateEmailTemp(attendee: any, notification: any):
    return Mail(
        from_email="sonthien17@gmail.com",
        to_emails=attendee[2],
        subject="Sonny test sending email",
        html_content=f"<p>This is the message for {attendee[0]} {attendee[1]}</p> <strong>{notification[0][0]}</strong>"
    )
    
def updateNotification(id: int, successNotification: int, cursor: any, connection: any):
    status = f"Notified {str(successNotification)} attendees"
    completed_date = datetime.utcnow()
    cursor.execute(f"UPDATE notification SET status = '{status}', completed_date='{completed_date}' WHERE id={id};")
    connection.commit()