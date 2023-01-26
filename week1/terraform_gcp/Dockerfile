FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:latest

WORKDIR /app
COPY . /app
COPY ./key_gcp/cacert.crt /usr/local/share/ca-certificates

RUN export GOOGLE_APPLICATION_CREDENTIALS="\app\key_gcp\ornate-shape-375811-98147f45dee9.json"
    

# Refresh token/session, and verify authentication
#RUN gcloud auth application-default login

ENTRYPOINT [ "bash" ]
