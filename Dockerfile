FROM python:3.7-buster
RUN groupadd --gid 1004 deploy \
    && useradd --home-dir /home/deploy --create-home --uid 1004 \
        --gid 1004 --shell /bin/sh --skel /dev/null deploy

ARG pip_username
ARG pip_password

WORKDIR /home/deploy
COPY .  ./
RUN chown -R deploy:deploy /home/deploy
USER deploy
#RUN chmod +x /home/deploy/gunicorn_starter.sh
RUN chmod +x /home/deploy/worker_k8s_job_watch.py
RUN PIP_USERNAME=$pip_username PIP_PASSWORD=$pip_password pip install --no-cache-dir -r requirements.txt -r internal_requirements.txt --user
ENV PATH="/home/deploy/.local/bin:${PATH}"
# CMD ["./gunicorn_starter.sh"]
