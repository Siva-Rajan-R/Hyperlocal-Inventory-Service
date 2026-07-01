import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_titan_email():
    # 1. Configure configuration and credentials
    smtp_server = "smtpout.secureserver.net"
    smtp_port = 465  # Use 465 for SSL, or 587 for TLS
    sender_email = "noreply@vexatech.in"
    sender_password = "Srini@2103"
    recipient_email = "vexatech.connect@gmail.com"

    
    # 2. Create the email message container
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = "Automated Report from Python"
    
    # 3. Define the email body (Supports Plain Text or HTML)
    body = """
    <html>
      <body>
        <h2>Hello!</h2>
        <p>This is an automated notification sent via <b>Titan Email</b> using Python.</p>
      </body>
    </html>
    """
    msg.attach(MIMEText(body, 'html'))
    
    try:
        # 4. Connect to Titan's secure SMTP server
        print("Connecting to Titan SMTP server...")
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            # Identify yourself to the server
            server.ehlo() 
            
            # Log in to your Titan account
            server.login(sender_email, sender_password)
            print("Login successful!")
            
            # Send the email
            server.sendmail(sender_email, recipient_email, msg.as_string())
            print(f"Email successfully sent to {recipient_email}!")
            
    except Exception as e:
        print(f"An error occurred while sending email: {e}")

# Run the function
if __name__ == "__main__":
    send_titan_email()
