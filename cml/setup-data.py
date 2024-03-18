!chmod 755 cml/setup-data.sh
#!cml/setup-data.sh

import subprocess
import sys
command = "bash cml/setup-data.sh"

try:
    # Execute the command
    result = subprocess.run(command, shell=True, check=False)

    print(result)
    # Check the return code
    if result.returncode != 0:
        raise Exception(f"Command failed with return code {result.returncode}")
except Exception as e:
    print(f"An error occurred: {e}")
    sys.exit(result.returncode)