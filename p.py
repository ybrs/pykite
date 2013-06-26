import pika
s = 'amqp://kite-api-kd-gzeywf4kjrodgx5f5fofbkxr5updg25jekpd8hyexv73fwudz7zvdpcewzekkevv:92fa3de5e550a5564e99ec43@local.koding.com:5672/%2F'
print s
parameters = pika.URLParameters(s)#'amqp://guest:guest@localhost:5672/%2F')
print parameters