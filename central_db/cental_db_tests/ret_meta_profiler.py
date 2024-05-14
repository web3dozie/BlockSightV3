import subprocess


def run_profiling():
    # Path to the Python script you want to profile
    script_path = "C:\\Users\\Dozie\\Desktop\\Code Projects\\BlockSightV3\\central_db\\cental_db_tests\\retrievemetaprofile.py"

    # Start py-spy
    cmd = [
        'py-spy', 'run',
        '--output', 'profile.svg',
        '--format', 'flamegraph',
        '--', 'python', script_path
    ]

    # Execute the command
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Wait for the process to complete
    stdout, stderr = process.communicate()

    if process.returncode == 0:
        print("Profiling complete, output saved to profile.svg")
    else:
        print(f"Error during profiling: {stderr.decode()}")

    return stdout.decode(), stderr.decode()


# Call the function to start profiling
run_profiling()
