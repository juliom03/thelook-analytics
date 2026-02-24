FROM python:3.11-slim

WORKDIR /app

# Copy dbt project
COPY thelook_analytics/ ./thelook_analytics/
COPY profiles_cloud.yml ./profiles.yml

# Install dbt
RUN pip install dbt-bigquery

# Run dbt
CMD ["dbt", "run", "--project-dir", "thelook_analytics", "--profiles-dir", "."]