FROM public.ecr.aws/lambda/python@sha256:3abb572d57e4988765dbd78b72027d2979024a9b46db52cdf67d53b10987577f AS dependencies

COPY requirements-lambda.txt /tmp/requirements-lambda.txt
COPY scripts/clean_lambda_task.py /tmp/clean_lambda_task.py
RUN python -m pip install \
      --disable-pip-version-check \
      --no-cache-dir \
      --require-hashes \
      --requirement /tmp/requirements-lambda.txt \
      --target "${LAMBDA_TASK_ROOT}"

COPY metropulse/ "${LAMBDA_TASK_ROOT}/metropulse/"
RUN python /tmp/clean_lambda_task.py "${LAMBDA_TASK_ROOT}"

FROM public.ecr.aws/lambda/python@sha256:3abb572d57e4988765dbd78b72027d2979024a9b46db52cdf67d53b10987577f AS runtime

LABEL org.opencontainers.image.title="MetroPulse validation Lambda" \
      org.opencontainers.image.description="Deterministic MetroPT-3 daily validation adapter" \
      org.opencontainers.image.base.name="public.ecr.aws/lambda/python@sha256:3abb572d57e4988765dbd78b72027d2979024a9b46db52cdf67d53b10987577f"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TMPDIR=/tmp

COPY --from=dependencies /var/task/ "${LAMBDA_TASK_ROOT}/"

# Lambda assigns its default least-privileged runtime user when USER is omitted.
CMD ["metropulse.aws.lambda_handler.lambda_handler"]
