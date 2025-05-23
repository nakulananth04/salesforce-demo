# Use AWS Lambda Python base image for container support
FROM public.ecr.aws/lambda/python:3.9

# Copy function code and dependencies (paths now relative to subfolder)
COPY $PROJECT_DIR/app.py ${LAMBDA_TASK_ROOT}
COPY $PROJECT_DIR/requirements.txt .

# Install dependencies
RUN python3.9 -m pip install -r requirements.txt -t ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (app.lambda_handler)
CMD ["app.lambda_handler"]