FROM public.ecr.aws/lambda/python:3.9

# Install system dependencies for pyiceberg (if any) and boto3
RUN yum install -y gcc gcc-c++ make

# Install Python dependencies (pyiceberg, boto3)
COPY requirements.txt .
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# Copy lambda handler code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/

# Command to run the handler
CMD ["lambda_function.lambda_handler"]
