import time
import json
from pulsar import Function


class CreateSchema(Function):
    def __init__(self) -> None:
        self.processing_stack = {}
        self.minio_stack = {}


    def process(self, input, context):
        msg = json.loads(input)
        try:
            if msg['type'] == 'processing_ml':
                self.processing_stack[msg['uuid']] = msg
            elif msg['type'] == 'minio_stack':
                self.minio_stack[msg['uuid']] = msg
            # verify if any message has finished processing
            self.verify(context=context)
        except Exception as e:
            print(e)

    def verify(self, context):
        for key in self.processing_stack.keys():
            if key in self.minio_stack:
                comment = self.merge_two_dicts(self.processing_stack[key], self.minio_stack[key])
                context.publish("persistent://public/default/complete-youtube", json.dumps(comment).encode('utf-8'))
                # remove from dict
                self.processing_stack.pop(key)
                self.minio_stack.pop(key)


    def merge_two_dicts(self, x, y):
        z = x.copy()   # start with keys and values of x
        z.update(y)    # modifies z with keys and values of y
        return z