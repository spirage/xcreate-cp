FROM debian:11
RUN apt update && apt install -y supervisor
COPY app /app
WORKDIR /app
COPY supervisord.conf /etc/supervisord.conf
EXPOSE 7980
WORKDIR /
CMD ["supervisord", "-c", "/etc/supervisord.conf"]

