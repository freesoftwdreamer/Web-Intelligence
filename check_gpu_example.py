import subprocess

def check_gpu_usage():
    try:
        # Query GPU memory usage
        memory_used = subprocess.check_output(["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"]).decode("utf-8").strip().split('\n')

        # Query GPU utilization
        gpu_utilization = subprocess.check_output(["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"]).decode("utf-8").strip().split('\n')

        # Print the results
        for i, (mem, util) in enumerate(zip(memory_used, gpu_utilization)):
            print(f"GPU {i}: Memory used: {mem} MB | GPU Utilization: {util} %")

    except subprocess.CalledProcessError as e:
        print(f"Error checking GPU usage: {e}")

if __name__ == "__main__":
    check_gpu_usage()
