
# Use pre-built selenium-python base image
FROM umihico/aws-lambda-selenium-python:3.10.12-selenium4.9.1

# copy scraper code
COPY . ${LAMBDA_TASK_ROOT}

# install dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
RUN --mount=type=cache,target=/root/.cache \
    pip3 install -r ${LAMBDA_TASK_ROOT}/requirements.txt

ENV APP_VERSION=1.0.0

CMD ["main.handler"]
