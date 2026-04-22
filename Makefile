# ───────────────────────────────────────────────────────────────────────────────
# AWS Data Lake Pipeline – Makefile
# Usage: make <target>
# ───────────────────────────────────────────────────────────────────────────────

.PHONY: help install test lint format security tf-init tf-plan tf-apply clean

ENV      ?= dev
REGION   ?= us-east-1
BUCKET   ?= data-lake-$(ENV)-bucket
TF_DIR   := infrastructure/terraform

help:
	@echo ""
	@echo "  AWS Data Lake Pipeline – Developer Commands"
	@echo "  ─────────────────────────────────────────────"
	@echo "  install      Install all Python dependencies"
	@echo "  test         Run unit tests with coverage"
	@echo "  lint         Lint Python code (ruff)"
	@echo "  format       Auto-format code (black + isort)"
	@echo "  security     Run security scan (bandit)"
	@echo "  tf-init      Init Terraform"
	@echo "  tf-plan      Terraform plan for ENV=$(ENV)"
	@echo "  tf-apply     Terraform apply for ENV=$(ENV)"
	@echo "  deploy-glue  Upload Glue scripts to S3"
	@echo "  deploy-lambda Deploy Lambda functions"
	@echo "  clean        Remove build artifacts"
	@echo ""

install:
	pip install --upgrade pip
	pip install -r requirements.txt -r requirements-dev.txt

test:
	pytest tests/ -v --tb=short \
		--cov=data_validation --cov=lambdas \
		--cov-report=term-missing \
		--cov-fail-under=85

lint:
	ruff check . --select E,W,F,I,N --ignore E501

format:
	black .
	isort .

security:
	bandit -r lambdas/ data_validation/ glue_jobs/ dags/ -ll
	safety check -r requirements.txt

tf-init:
	cd $(TF_DIR) && terraform init

tf-plan:
	cd $(TF_DIR) && terraform plan -var="environment=$(ENV)" -var="aws_region=$(REGION)"

tf-apply:
	cd $(TF_DIR) && terraform apply -var="environment=$(ENV)" -var="aws_region=$(REGION)"

deploy-glue:
	aws s3 cp glue_jobs/raw_to_cleaned.py   s3://$(BUCKET)/glue_scripts/ --region $(REGION)
	aws s3 cp glue_jobs/cleaned_to_curated.py s3://$(BUCKET)/glue_scripts/ --region $(REGION)
	@echo "✅  Glue scripts deployed to s3://$(BUCKET)/glue_scripts/"

deploy-lambda:
	cd lambdas/ingestion_lambda && zip -r ../..//tmp/ingestion.zip .
	cd lambdas/validation_lambda && zip -r /tmp/validation.zip .
	aws lambda update-function-code \
		--function-name data-lake-$(ENV)-api-ingestion \
		--zip-file fileb:///tmp/ingestion.zip --region $(REGION)
	aws lambda update-function-code \
		--function-name data-lake-$(ENV)-data-validator \
		--zip-file fileb:///tmp/validation.zip --region $(REGION)
	@echo "✅  Lambda functions deployed"

trigger-pipeline:
	aws lambda invoke \
		--function-name data-lake-$(ENV)-api-ingestion \
		--payload '{"execution_date":"$(shell date +%Y-%m-%d)"}' \
		--region $(REGION) \
		/tmp/ingestion_response.json
	cat /tmp/ingestion_response.json

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache"  -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info"     -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage coverage.xml htmlcov/ /tmp/*.zip
	@echo "🧹  Clean complete"
