#specify the base image
FROM python:3.13.0

#tell the builder to copy files from the host and put them into the container image
COPY . ./reconciliation_of_mutual_settlements

#specify the path in the image where files will be copied and commands will be executed
WORKDIR /reconciliation_of_mutual_settlements

#tell the builder to run the specified command
RUN pip install -r requirements.txt && mkdir ./files

#set an environment variable that a running container will use
#ENV <name> <value>

#set the configuration on the image that indicates a port the image would like to expose
#EXPOSE <port-number>

#set the default user for all subsequent instructions
#USER <user-or-uid>

#set the default command a container using this image will run
CMD ["python", "main.py"]