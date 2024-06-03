import ray
import torch
import time

ray.init(address='auto')

@ray.remote(num_gpus=1)
def gpu_task():
    # Check if CUDA (GPU support) is available in PyTorch
    if torch.cuda.is_available():
        # Create a random tensor and move it to GPU
        data = torch.randn([1000, 1000]).cuda()
        print("Random data generated using torch.randn...")
        # Perform a simple computation (matrix multiplication) on the GPU
        result = torch.matmul(data, data.t())
        print("Matrix multiplication done with transpose of earlier matrix...")
        # Simulate some processing time
        print("sleeping using time.sleep()...")
        time.sleep(1)
        # Print a success message
        print("This task successfully performed a GPU-accelerated computation.")
    else:
        print("CUDA is not available. This task did not run on a GPU.")

gpu_future = gpu_task.remote()

for _ in range(5):
    ray.get(gpu_future)
    time.sleep(3)

