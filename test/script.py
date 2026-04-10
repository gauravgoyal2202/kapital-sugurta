import os
from datetime import datetime

def create_files_in_script_dir():
    # Get directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # File paths in the SAME directory
    txt_file_path = os.path.join(script_dir, "output.txt")
    log_file_path = os.path.join(script_dir, "execution.log")
    
    # Timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # File contents
    txt_content = f"This is a text file.\nCreated at: {timestamp}\n"
    log_content = f"[{timestamp}] Script executed successfully.\n"
    
    # Create/write TXT file
    with open(txt_file_path, "w") as txt_file:
        txt_file.write(txt_content)
    
    # Create/write LOG file
    with open(log_file_path, "a") as log_file:  # append mode for logs
        log_file.write(log_content)
    
    print("Files created successfully in script directory.")
    print(f"TXT: {txt_file_path}")
    print(f"LOG: {log_file_path}")

if __name__ == "__main__":
    create_files_in_script_dir()
