# gunicorn_config.py
workers = 4
threads = 2
bind = '0.0.0.0:6063'
daemon = 'false'
worker_connections = 5
accesslog = 'access.log'
errorlog = 'error.log'
loglevel = 'info'
