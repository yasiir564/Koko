from flask import Flask, request, jsonify, send_file
import os
import uuid
import re
import subprocess
import mimetypes
from werkzeug.utils import secure_filename

app = Flask(__name__)

# Configuration - Use absolute paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(CURRENT_DIR, "uploads/")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "converted/")
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB max file size

# Create directories if they don't exist
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def sanitize_filename(name):
    """Remove any path info and sanitize the file name"""
    # Get the base name
    name = os.path.basename(name)
    # Replace spaces with underscores
    name = name.replace(' ', '_')
    # Remove special characters
    name = re.sub(r'[^A-Za-z0-9_\-\.]', '', name)
    return name

def generate_unique_filename(original_name):
    """Generate a unique filename based on the original name"""
    filename, extension = os.path.splitext(original_name)
    unique_id = uuid.uuid4().hex[:10]
    return f"{sanitize_filename(filename)}_{unique_id}{extension}"

@app.route('/convert', methods=['POST', 'OPTIONS'])
def convert_video():
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 200
    
    response = {}
    
    # Check if file is uploaded
    if 'video' not in request.files:
        response['success'] = False
        response['error'] = "No file uploaded"
        return jsonify(response), 400
    
    video_file = request.files['video']
    
    # Check if filename is empty
    if video_file.filename == '':
        response['success'] = False
        response['error'] = "No file selected"
        return jsonify(response), 400
    
    # Validate file size (Flask doesn't have built-in size checking before reading)
    video_file.seek(0, os.SEEK_END)
    file_size = video_file.tell()
    video_file.seek(0)  # Reset file pointer
    
    if file_size > MAX_FILE_SIZE:
        response['success'] = False
        response['error'] = f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)}MB"
        return jsonify(response), 400
    
    try:
        # Generate unique filenames
        unique_video_name = generate_unique_filename(video_file.filename)
        video_path = os.path.join(UPLOAD_DIR, unique_video_name)
        
        # Generate output filename (replace extension with mp3)
        output_filename = os.path.splitext(unique_video_name)[0] + '.mp3'
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        # Save the uploaded file
        video_file.save(video_path)
        
        # Convert the video to MP3 using FFmpeg
        ffmpeg_command = [
            "ffmpeg", 
            "-i", video_path, 
            "-vn", 
            "-ar", "44100", 
            "-ac", "2", 
            "-b:a", "192k", 
            output_path
        ]
        
        process = subprocess.run(
            ffmpeg_command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Check if conversion was successful
        if process.returncode != 0:
            raise Exception(f"FFmpeg conversion failed: {process.stderr}")
        
        # Check if output file exists
        if not os.path.exists(output_path):
            raise Exception("Output file was not created")
        
        # Debug information
        print(f"File saved to: {output_path}")
        print(f"File exists: {os.path.exists(output_path)}")
        
        # Return success response with direct file path
        response['success'] = True
        response['filename'] = output_filename
        
        # Clean up the original file (optional)
        os.remove(video_path)
        
        return jsonify(response)
        
    except Exception as e:
        # Delete the uploaded file if there was an error
        if 'video_path' in locals() and os.path.exists(video_path):
            os.remove(video_path)
        
        response['success'] = False
        response['error'] = str(e)
        return jsonify(response), 500

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    """Serve the converted files directly"""
    file_path = os.path.join(OUTPUT_DIR, filename)
    
    # Debug information
    print(f"Download request for: {filename}")
    print(f"Full path: {file_path}")
    print(f"File exists: {os.path.exists(file_path)}")
    
    if os.path.exists(file_path):
        return send_file(
            file_path, 
            as_attachment=True,
            download_name=filename,
            mimetype='audio/mpeg'
        )
    else:
        return f"File not found: {filename}", 404

# Add CORS headers to all responses
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'  # Change to your specific domain in production
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

# Route to display all available MP3 files
@app.route('/files', methods=['GET'])
def list_files():
    files = []
    for filename in os.listdir(OUTPUT_DIR):
        if filename.endswith('.mp3'):
            files.append(filename)
    return jsonify({"files": files})

if __name__ == '__main__':
    app.run(debug=True)
