#!/bin/sh
gunicorn --preload  -c gunicorn_config.py "app:main()"