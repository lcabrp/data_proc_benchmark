import platform
import socket
import psutil
from datetime import datetime

def get_host_info() -> dict:
    """Collect comprehensive host system information as a dict, with Windows-specific enhancements and GPU info."""
    info = {
        'timestamp': datetime.now().isoformat(),
        'hostname': socket.gethostname(),
        'platform': platform.platform(),
        'system': platform.system(),
        'release': platform.release(),
        'version': platform.version(),
        'machine': platform.machine(),
        'processor': platform.processor(),
        'cpu_count_logical': psutil.cpu_count(logical=True),
        'cpu_count_physical': psutil.cpu_count(logical=False),
        'cpu_freq_max': psutil.cpu_freq().max if psutil.cpu_freq() else 'N/A',
        'cpu_freq_current': psutil.cpu_freq().current if psutil.cpu_freq() else 'N/A',
        'memory_total_gb': round(psutil.virtual_memory().total / (1024**3), 2),
        'memory_available_gb': round(psutil.virtual_memory().available / (1024**3), 2),
        'python_version': platform.python_version(),
        'python_implementation': platform.python_implementation(),
        'cpu_brand': 'Unknown',
        'cpu_arch': 'Unknown',
        'gpu_name': 'Unknown',
        'gpu_memory_total_mb': 'Unknown'
    }

    # Windows-specific CPU info using WMI
    if info['system'] == 'Windows':
        try:
            import wmi
            w = wmi.WMI()
            cpu = w.Win32_Processor()[0]
            info['cpu_brand'] = cpu.Name.strip()
            arch_map = {
                0: 'x86',
                1: 'MIPS',
                2: 'Alpha',
                3: 'PowerPC',
                5: 'ARM',
                6: 'Itanium',
                9: 'x64'
            }
            info['cpu_arch'] = arch_map.get(cpu.Architecture, 'Unknown')
            # GPU info via WMI
            try:
                gpus = w.Win32_VideoController()
                if gpus:
                    # Pick GPU with largest AdapterRAM
                    best_gpu = None
                    max_ram = 0
                    for gpu in gpus:
                        ram = int(gpu.AdapterRAM) if gpu.AdapterRAM else 0
                        if ram > max_ram:
                            max_ram = ram
                            best_gpu = gpu
                    if best_gpu:
                        info['gpu_name'] = best_gpu.Name.strip()
                        info['gpu_memory_total_mb'] = max_ram // (1024**2)
                    else:
                        info['gpu_name'] = 'Unknown'
                        info['gpu_memory_total_mb'] = 'Unknown'
                else:
                    info['gpu_name'] = 'Unknown'
                    info['gpu_memory_total_mb'] = 'Unknown'
            except Exception:
                info['gpu_name'] = 'Unknown'
                info['gpu_memory_total_mb'] = 'Unknown'
        except Exception:
            # Fallback to platform.processor if WMI fails
            info['cpu_brand'] = info['processor']
            info['cpu_arch'] = info['machine']
    else:
        # Try cpuinfo for Linux/macOS
        try:
            import cpuinfo
            cpu_info = cpuinfo.get_cpu_info()
            info['cpu_brand'] = cpu_info.get('brand_raw', 'Unknown')
            info['cpu_arch'] = cpu_info.get('arch', 'Unknown')
        except ImportError:
            info['cpu_brand'] = 'Unknown (cpuinfo not available)'
            info['cpu_arch'] = 'Unknown'
        except Exception:
            info['cpu_brand'] = info['processor']
            info['cpu_arch'] = info['machine']
        # GPU info for Linux/macOS using GPUtil
        try:
            import GPUtil
            gpus = GPUtil.getGPUs()
            if gpus:
                info['gpu_name'] = gpus[0].name
                info['gpu_memory_total_mb'] = int(gpus[0].memoryTotal)
        except Exception:
            info['gpu_name'] = 'Unknown'
            info['gpu_memory_total_mb'] = 'Unknown'

    # Normalize processor string for vendor/model/family (optional)
    proc_str = info.get('processor', '')
    if proc_str:
        if 'GenuineIntel' in proc_str:
            info['cpu_vendor'] = 'Intel'
        elif 'AuthenticAMD' in proc_str:
            info['cpu_vendor'] = 'AMD'
        else:
            info['cpu_vendor'] = 'Unknown'
        # Extract family/model if possible
        import re
        match = re.search(r'Family (\d+) Model (\d+)', proc_str)
        if match:
            info['cpu_family'] = match.group(1)
            info['cpu_model'] = match.group(2)
        else:
            info['cpu_family'] = ''
            info['cpu_model'] = ''
    else:
        info['cpu_vendor'] = ''
        info['cpu_family'] = ''
        info['cpu_model'] = ''

    return info

if __name__ == "__main__":
    host_info = get_host_info()
    for key, value in host_info.items():
        print(f"{key}: {value}")