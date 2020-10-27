FROM python:3.8.5
COPY . /app
WORKDIR /app
RUN apt-get update
RUN apt-get install -y python3-pip
RUN pip3 install -r requirements.txt
COPY docker_entrypoint.sh /app
RUN chmod +rwx location_search.py
ENTRYPOINT ["python"]
CMD ["location_search.py"]
EXPOSE 7643
