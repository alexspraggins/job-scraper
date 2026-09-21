"""Email delivery for scraper output."""

import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_email(
    file_path: str,
    recipient_email: str,
    from_email: str,
    email_password: str,
    email_smtp: str,
) -> None:
    message = MIMEMultipart()
    message["From"] = from_email
    message["To"] = recipient_email
    message["Subject"] = "Newly Scraped Job Listings"
    message.attach(MIMEText("Attached is the latest scrape of job listings.", "plain"))

    with open(file_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f"attachment; filename={os.path.basename(file_path)}",
    )
    message.attach(part)

    with smtplib.SMTP(email_smtp, 587) as server:
        server.starttls()
        server.login(from_email, email_password)
        server.sendmail(from_email, recipient_email, message.as_string())

    print("Email sent successfully!")
