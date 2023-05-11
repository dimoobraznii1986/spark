release:
	aws s3 cp src/cloudflare_logs.py  s3://datalake/glue/

delete_job:
	aws glue delete-job \
    --region eu-central-1 \
    --job-name app_logs_glue_job

create_job:
	aws glue create-job \
        --region eu-central-1 \
        --name app_logs_glue_job \
        --role arn:aws:iam::123567890000:role/GlueServiceRole \
        --execution-property '{"MaxConcurrentRuns": 24}' \
        --command '{"Name": "glueetl", "ScriptLocation": "s3://datalake/glue/logs.py", "PythonVersion": "3"}' \
        --default-arguments '{"--source_s3_bucket": "auth0-datalake", "--target_s3_bucket": "datalake", "--target_s3_prefix": "applogs-glue/stg_app_logs__access_logs/"}' \
        --worker-type G.2X \
        --number-of-workers 10 \
        --glue-version 4.0 \
        --max-retries 0

job_definition:
	aws glue get-job \
		--region eu-central-1 \
		--job-name app_logs_glue_job


run_24_hours:
	collector_date="20230428"; \
	for h in {0..23}; do \
		hour=$$(printf "%02d" $$h); \
		echo -n "$$collector_date-$$hour "; aws glue start-job-run --region eu-central-1 --job-name app_logs_glue_job --arguments="{\"--collector_date\": \"$$collector_date\", \"--collector_hour\": \"$$hour\"}" --query JobRunId --output text | tee -a job-ids-app-logs.txt; \
	done


check_last_batch_status:
	(printf "COLLECTOR_DATE-HOUR\tJOB_STATE\tSTARTED_AT\tEXEC_TIME\n"; cat job-ids-app-logs.txt | xargs -P 4 -n2 -- bash -c 'printf "$$0\t%s\n" "$$(aws --no-cli-pager glue get-job-run --region eu-central-1 --job-name app_logs_glue_job --query "JobRun.[JobRunState, StartedOn, ExecutionTime]" --output text --run-id $$1)"') | tee job-status-app-logs.txt

start_single_job:
	aws glue start-job-run --region eu-central-1 --job-name app_logs_glue_job --arguments '{"--collector_date": "20230428", "--collector_hour": "12"}' --query JobRunId --output text | tee job-id.current

check_single_job_status:
	aws glue get-job-run --region eu-central-1 --job-name app_logs_glue_job --run-id $(cat job-id.current)
