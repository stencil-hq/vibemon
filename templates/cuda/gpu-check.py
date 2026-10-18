#!/usr/bin/env python3
"""vmon-gpu-check: prove the sandbox's GPUs work (cuda template).

Lists every GPU through the CUDA driver API (`libcuda`, from the node's
guest driver bundle) and round-trips a buffer through device memory with the
CUDA runtime (`libcudart`, from the image). Needs only the Python standard
library. Exits 0 when every visible GPU passes, 1 otherwise.
"""

import ctypes
import glob
import sys

PATTERN = bytes(range(256)) * 4096  # 1 MiB


def load(names):
	for name in names:
		try:
			return ctypes.CDLL(name)
		except OSError:
			continue
	sys.exit(f"vmon-gpu-check: cannot load any of {names}")


def check(what, status):
	if status != 0:
		sys.exit(f"vmon-gpu-check: {what} failed with error {status}")


def main():
	cuda = load(["libcuda.so.1", "libcuda.so"])
	check("cuInit", cuda.cuInit(0))
	version = ctypes.c_int()
	check("cuDriverGetVersion", cuda.cuDriverGetVersion(ctypes.byref(version)))
	count = ctypes.c_int()
	check("cuDeviceGetCount", cuda.cuDeviceGetCount(ctypes.byref(count)))
	print(f"CUDA driver API {version.value // 1000}.{version.value % 1000 // 10}, {count.value} GPU(s)")
	if count.value == 0:
		sys.exit("vmon-gpu-check: no GPU visible (was the sandbox created with GPUs?)")
	for index in range(count.value):
		device = ctypes.c_int()
		check("cuDeviceGet", cuda.cuDeviceGet(ctypes.byref(device), index))
		name = ctypes.create_string_buffer(256)
		check("cuDeviceGetName", cuda.cuDeviceGetName(name, len(name), device))
		total = ctypes.c_size_t()
		check("cuDeviceTotalMem", cuda.cuDeviceTotalMem_v2(ctypes.byref(total), device))
		print(f"GPU {index}: {name.value.decode()} ({total.value >> 20} MiB)")

	runtime = load(
		["libcudart.so.13", "libcudart.so.12", "libcudart.so"]
		+ sorted(glob.glob("/usr/local/cuda*/lib64/libcudart.so*"), reverse=True)
	)
	runtime_version = ctypes.c_int()
	check("cudaRuntimeGetVersion", runtime.cudaRuntimeGetVersion(ctypes.byref(runtime_version)))
	major, minor = runtime_version.value // 1000, runtime_version.value % 1000 // 10
	print(f"CUDA runtime {major}.{minor}")
	host_to_device, device_to_host = 1, 2
	for index in range(count.value):
		check("cudaSetDevice", runtime.cudaSetDevice(index))
		buffer = ctypes.c_void_p()
		check("cudaMalloc", runtime.cudaMalloc(ctypes.byref(buffer), ctypes.c_size_t(len(PATTERN))))
		source = ctypes.create_string_buffer(PATTERN, len(PATTERN))
		back = ctypes.create_string_buffer(len(PATTERN))
		size = ctypes.c_size_t(len(PATTERN))
		check("cudaMemcpy to device", runtime.cudaMemcpy(buffer, source, size, host_to_device))
		check("cudaMemcpy to host", runtime.cudaMemcpy(back, buffer, size, device_to_host))
		check("cudaFree", runtime.cudaFree(buffer))
		if back.raw != PATTERN:
			sys.exit(f"vmon-gpu-check: GPU {index} returned corrupted data")
		print(f"GPU {index}: 1 MiB device memory round trip ok")
	print("ok")


if __name__ == "__main__":
	main()
