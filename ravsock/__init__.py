import platform
import subprocess

def getProcessorMacM1():
    # Check for Apple Mac
    if platform.system() == 'Darwin':
        chipset =  subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string']).decode('utf-8').strip()
        if chipset == "Apple M1":
            return True
        else:
            return False
    else:
        return None

if not getProcessorMacM1():
    from .encryption import get_context
