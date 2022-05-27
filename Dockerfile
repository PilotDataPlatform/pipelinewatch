FROM python:3.7-buster
RUN groupadd --gid 1004 deploy && \
    useradd --home-dir /home/deploy \
            --create-home --uid 1004 \
            --gid 1004 --shell /bin/sh \
            --skel /dev/null deploy

RUN pip install --no-cache-dir poetry==1.1.12

WORKDIR /home/deploy
COPY .  ./

RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-root --no-interaction

RUN chown -R deploy:deploy /home/deploy
USER deploy
RUN chmod +x /home/deploy/worker_k8s_job_watch.py

ENV PATH="/home/deploy/.local/bin:${PATH}"

CMD ["/home/deploy/worker_k8s_job_watch.py"]
