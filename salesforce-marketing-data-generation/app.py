import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
import os

faker = Faker()
s3 = boto3.client('s3')
bucket = 'salesforce-marketing-raw-data'

print("Start")

# === Row counts (edit these to control volume) ===
NUM_ACCOUNTS = int(os.environ['NUM_ACCOUNTS'])
NUM_CAMPAIGNS = int(os.environ['NUM_CAMPAIGNS'])
NUM_CONTACTS = int(os.environ['NUM_CONTACTS'])
NUM_LEADS = int(os.environ['NUM_LEADS'])
NUM_CAMPAIGN_MEMBERS = int(os.environ['NUM_CAMPAIGN_MEMBERS'])
NUM_EMAIL_TEMPLATES = int(os.environ['NUM_EMAIL_TEMPLATES'])
NUM_EMAIL_SENDS = int(os.environ['NUM_EMAIL_SENDS'])
NUM_EMAIL_ENGAGEMENTS = int(os.environ['NUM_EMAIL_ENGAGEMENTS'])
NUM_EVENTS = int(os.environ['NUM_EVENTS'])
NUM_OPPORTUNITIES = int(os.environ['NUM_OPPORTUNITIES'])

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_timestamp(start_date, end_date):
    # Calculate random days between dates
    delta_days = (end_date.date() - start_date.date()).days
    random_days = random.randint(0, delta_days)
    
    # Calculate random seconds within a day (86400 seconds = 24 hours)
    random_seconds = random.randint(0, 86399)
    random_microseconds = random.randint(0, 999999)
    
    # Combine to create final timestamp
    return (
        start_date + 
        timedelta(days=random_days) + 
        timedelta(seconds=random_seconds) +
        timedelta(microseconds=random_microseconds)
    )

def random_id():
    return uuid.uuid4().hex[:18].upper()

def generate_timestamp():
    return datetime.now().strftime('%Y-%m-%d_%H%M%S')

def upload_parquet(df, key):
    print(f"Original key: {key}")
    key = key.lower()
    print(f"Lowercase key used for upload: {key}")
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.upload_fileobj(buf, bucket, key)

def lambda_handler(event, context):
    timestamp = generate_timestamp()
    base_path = f"{timestamp}/"

    df_account = pd.DataFrame([{
        "ACCOUNT_ID": random_id(),
        "ACCOUNT_NAME": faker.company(),
        "INDUSTRY": faker.job(),
        "WEBSITE": faker.url(),
        "ANNUAL_REVENUE": round(random.uniform(1e5, 1e7), 2),
        "CREATED_DATE": random_timestamp(datetime(2020, 1, 1), datetime(2023, 12, 31))
    } for _ in range(NUM_ACCOUNTS)])
    upload_parquet(df_account, base_path + "account.parquet")

    df_campaign = pd.DataFrame([{
        "CAMPAIGN_ID": random_id(),
        "CAMPAIGN_NAME": faker.catch_phrase(),
        "START_DATE": (start := random_timestamp(datetime.today() - timedelta(days=365), datetime.today())),
        "END_DATE": start + timedelta(days=random.randint(7, 90)),
        "STATUS": random.choice(["Planned", "In Progress", "Completed"]),
        "BUDGET": round(random.uniform(1000, 100000), 2)
    } for _ in range(NUM_CAMPAIGNS)])
    upload_parquet(df_campaign, base_path + "campaign.parquet")

    df_contact = pd.DataFrame([{
        "CONTACT_ID": random_id(),
        "FIRST_NAME": faker.first_name(),
        "LAST_NAME": faker.last_name(),
        "EMAIL": faker.email(),
        "PHONE": faker.phone_number(),
        "CREATED_DATE": random_timestamp(datetime(2020, 1, 1), datetime(2023, 12, 31))
    } for _ in range(NUM_CONTACTS)])
    upload_parquet(df_contact, base_path + "contact.parquet")

    df_lead = pd.DataFrame([{
        "LEAD_ID": random_id(),
        "FIRST_NAME": faker.first_name(),
        "LAST_NAME": faker.last_name(),
        "COMPANY": faker.company(),
        "EMAIL": faker.email(),
        "PHONE": faker.phone_number(),
        "STATUS": random.choice(["New", "Working", "Qualified"]),
        "CONVERTED_CONTACT_ID": random.choice(df_contact["CONTACT_ID"]),
        "CREATED_DATE": random_timestamp(datetime(2020, 1, 1), datetime(2023, 12, 31))
    } for _ in range(NUM_LEADS)])
    upload_parquet(df_lead, base_path + "lead.parquet")

    df_campaign_member = pd.DataFrame([{
        "MEMBER_ID": random_id(),
        "CAMPAIGN_ID": random.choice(df_campaign["CAMPAIGN_ID"]),
        "CONTACT_ID": random.choice(df_contact["CONTACT_ID"]),
        "LEAD_ID": random.choice(df_lead["LEAD_ID"]),
        "STATUS": random.choice(["Sent", "Responded"]),
        "FIRST_RESPONDED_DATE": random_timestamp(datetime(2020, 1, 1), datetime(2023, 12, 31)),
        "CREATED_DATE": random_timestamp(datetime(2020, 1, 1), datetime(2023, 12, 31))
    } for _ in range(NUM_CAMPAIGN_MEMBERS)])
    upload_parquet(df_campaign_member, base_path + "campaign_member.parquet")

    df_template = pd.DataFrame([{
        "TEMPLATE_ID": random_id(),
        "TEMPLATE_NAME": faker.bs(),
        "SUBJECT": faker.catch_phrase(),
        "HTML_CONTENT": f"<html><body><h1>{faker.catch_phrase()}</h1></body></html>",
        "CREATED_DATE": random_timestamp(datetime(2020, 1, 1), datetime(2023, 12, 31))
    } for _ in range(NUM_EMAIL_TEMPLATES)])
    upload_parquet(df_template, base_path + "email_template.parquet")

    df_send = pd.DataFrame([{
        "SEND_ID": random_id(),
        "CAMPAIGN_ID": random.choice(df_campaign["CAMPAIGN_ID"]),
        "EMAIL_TEMPLATE_ID": random.choice(df_template["TEMPLATE_ID"]),
        "SEND_DATE": random_timestamp(datetime(2020, 1, 1), datetime(2023, 12, 31)),
        "SUBJECT_LINE": faker.catch_phrase(),
        "TOTAL_SENT": random.randint(50, 5000)
    } for _ in range(NUM_EMAIL_SENDS)])
    upload_parquet(df_send, base_path + "email_send.parquet")

    df_engagement = pd.DataFrame([{
        "ENGAGEMENT_ID": random_id(),
        "SEND_ID": random.choice(df_send["SEND_ID"]),
        "CONTACT_ID": random.choice(df_contact["CONTACT_ID"]),
        "CONTACT_SK": uuid.uuid4().hex[:32],
        "ENGAGEMENT_TYPE": random.choice(["Open", "Click"]),
        "ENGAGEMENT_TIMESTAMP": faker.date_time_this_year(),
        "LINK_URL": faker.url()
    } for _ in range(NUM_EMAIL_ENGAGEMENTS)])
    upload_parquet(df_engagement, base_path + "email_engagement.parquet")

    df_event = pd.DataFrame([{
        "EVENT_ID": random_id(),
        "SUBJECT": faker.catch_phrase(),
        "START_DATE_TIME": (start := random_timestamp(datetime.today() - timedelta(days=365), datetime.today())),
        "END_DATE_TIME": start + timedelta(hours=2),
        "TYPE": random.choice(["Webinar", "Meeting", "Demo"]),
        "RELATED_CAMPAIGN_ID": random.choice(df_campaign["CAMPAIGN_ID"]),
        "RELATED_CONTACT_ID": random.choice(df_contact["CONTACT_ID"]),
        "RELATED_LEAD_ID": random.choice(df_lead["LEAD_ID"]),
        "CREATED_DATE": random_timestamp(datetime(2020, 1, 1), datetime(2023, 12, 31))
    } for _ in range(NUM_EVENTS)])
    upload_parquet(df_event, base_path + "event.parquet")

    df_opportunity = pd.DataFrame([{
        "OPPORTUNITY_ID": random_id(),
        "OPPORTUNITY_NAME": faker.catch_phrase(),
        "ACCOUNT_ID": random.choice(df_account["ACCOUNT_ID"]),
        "STAGE_NAME": random.choice(["Prospecting", "Qualification", "Proposal", "Closed Won", "Closed Lost"]),
        "AMOUNT": round(random.uniform(5000, 100000), 2),
        "CLOSE_DATE": random_timestamp(datetime.today(), datetime.today() + timedelta(days=180)),
        "CAMPAIGN_ID": random.choice(df_campaign["CAMPAIGN_ID"]),
        "CREATED_DATE": random_timestamp(datetime(2020, 1, 1), datetime(2023, 12, 31))
    } for _ in range(NUM_OPPORTUNITIES)])
    upload_parquet(df_opportunity, base_path + "opportunity.parquet")

    return {
        'statusCode': 200,
        'body': f'Sample data uploaded to s3://{bucket}/{base_path}'
    }

