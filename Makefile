# zip-lambda:
# 	zip -r .venv/lib/python3.7/site-packages/ ./packages.zip && zip -g packages.zip lambda_function.py

zip-lambda:
	zip -g packages.zip lambda_function.py aws_lambda_utils.py aws_secret_manager_utils.py \
		bigquery_transferer.py bigquery_utils.py google_auth_utils.py



update-lambda:
	aws lambda update-function-code --function-name akiranoda-test-bq-transfer --zip-file fileb://packages.zip