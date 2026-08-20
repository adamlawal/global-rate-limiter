FROM python:3.13-alpine

WORKDIR /app
COPY src ./src
COPY public ./public

RUN addgroup -S app && adduser -S app -G app && chown -R app:app /app

USER app
EXPOSE 8080
CMD ["python", "-m", "src.main"]
