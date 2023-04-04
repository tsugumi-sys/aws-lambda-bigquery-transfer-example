zip-lambda:
	zip -r .venv/lib/python3.7/site-packages/ ./packages.zip && zip -g packages.zip lambda_function.py


update-lambda:
	aws lambda update-function-code --function-name akiranoda-test-bq-transfer --zip-file fileb://packages.zip