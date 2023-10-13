import json
import os

from kafka import KafkaProducer


def init_context(context):
    processing_stack = {}
    minio_stack = {}

    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)
    setattr(context, "processing_stack", processing_stack)
    setattr(context, "minio_stack", minio_stack)


def handler(context, event):
    msg = json.loads(event.body.decode("utf-8"))
    
    try:
        if msg["type"] == "processing_ml":
            context.processing_stack[msg["uuid"]] = msg
        elif msg["type"] == "minio_stack":
            context.minio_stack[msg["uuid"]] = msg
        # verify if any message has finished processing
        verify(context=context)
    except Exception as e:
        print("Error: {}".format(e))


def verify(context):
    for key in context.processing_stack.keys():
        if key in context.minio_stack:
            comment = merge_two_dicts(
                context.processing_stack[key], context.minio_stack[key]
            )
            context.producer.send("complete-youtube", value=comment)
            # remove from dict
            context.processing_stack.pop(key)
            context.minio_stack.pop(key)


def merge_two_dicts(x, y):
    z = x.copy()  # start with keys and values of x
    z.update(y)  # modifies z with keys and values of y
    return z
