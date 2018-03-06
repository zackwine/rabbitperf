# Rabbitmq Performance Utility
Rabbitmq verification tool


# Development

Running the rabbitmq container:


docker run -d -it -p 5672:5672 --hostname my-rabbit --name some-rabbit -e RABBITMQ_DEFAULT_USER=sensu -e RABBITMQ_DEFAULT_PASS=cisco123 rabbitmq:3-management

