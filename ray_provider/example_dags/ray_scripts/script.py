# script.py
import ray

@ray.remote
def hello_world():
    return "hello world"

ray.init()
print(ray.get(hello_world.remote()))