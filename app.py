from flask import Flask, request, jsonify, send_file
import os
import uuid
import re
import subprocess
import mimetypes
import time
import threading
import logging
from werkzeug.utils import secure_filename
from functools import lru_cache

app = Flask(__name__)

# Configuration - Use absolute paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(CURRENT_DIR, "uploads/")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "converted/")
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB max file size
CACHE_EXPIRY = 3600  # Files expire after 1 hour (in seconds)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create directories if they don't exist
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# In-memory cache for recent conversions
# Structure: {unique_id: {"output_path": path, "last_accessed": timestamp}}
file_cache = {}
cache_lock = threading.Lock()

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

@lru_cache(maxsize=10)
def get_ffmpeg_version():
    """Cache the FFmpeg version to avoid repeated subprocess calls"""
    try:
        process = subprocess.run(
            ["ffmpeg", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return process.stdout.split('\n')[0]
    except Exception as e:
        return f"FFmpeg version check failed: {str(e)}"

def cleanup_expired_files():
    """Remove files that haven't been accessed for CACHE_EXPIRY seconds"""
    current_time = time.time()
    with cache_lock:
        expired_keys = []
        for key, data in file_cache.items():
            if current_time - data["last_accessed"] > CACHE_EXPIRY:
                try:
                    # Remove the file if it exists
                    if os.path.exists(data["output_path"]):
                        os.remove(data["output_path"])
                        logger.info(f"Removed expired file: {data['output_path']}")
                    expired_keys.append(key)
                except Exception as e:
                    logger.error(f"Error removing file {data['output_path']}: {str(e)}")
        
        # Remove expired entries from cache
        for key in expired_keys:
            del file_cache[key]

# Start a cleanup thread
def start_cleanup_thread():
    """Start a background thread to periodically clean up expired files"""
    def cleanup_task():
        while True:
            cleanup_expired_files()
            time.sleep(300)  # Run every 5 minutes

    cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
    cleanup_thread.start()

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
        
        # Check if we have a cached version based on file content hash (not implemented for brevity)
        # For a complete implementation, you could hash the file and check if it exists in cache
        
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
        
        # Add to cache
        file_id = os.path.splitext(output_filename)[0]
        with cache_lock:
            file_cache[file_id] = {
                "output_path": output_path,
                "last_accessed": time.time()
            }
        
        logger.info(f"File converted and cached: {output_path}")
        
        # Clean up the original file
        os.remove(video_path)
        
        # Return success response
        response['success'] = True
        response['filename'] = output_filename
        
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
    file_id = os.path.splitext(filename)[0]
    
    # Update last accessed time in cache
    with cache_lock:
        if file_id in file_cache:
            file_cache[file_id]["last_accessed"] = time.time()
    
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
    response.headers['Access-Control-Allow-Origin'] = 'https://tokhaste.com'  # Change to your specific domain in production
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

# Route to display all available MP3 files (only shows cached files)
@app.route('/files', methods=['GET'])
def list_files():
    files = []
    with cache_lock:
        for file_id, data in file_cache.items():
            filename = os.path.basename(data["output_path"])
            if os.path.exists(data["output_path"]) and filename.endswith('.mp3'):
                files.append(filename)
    return jsonify({"files": files})

@app.route('/status', methods=['GET'])
def status():
    """Provides status information about the service"""
    with cache_lock:
        cache_count = len(file_cache)
        cached_files = [os.path.basename(data["output_path"]) for data in file_cache.values()]
    
    return jsonify({
        "status": "running",
        "ffmpeg_version": get_ffmpeg_version(),
        "cached_files_count": cache_count,
        "cache_expiry_seconds": CACHE_EXPIRY
    })

@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    """Admin endpoint to manually clear the cache"""
    try:
        cleanup_expired_files()
        return jsonify({"success": True, "message": "Cache cleanup triggered"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    # Start the cleanup thread when the app starts
    start_cleanup_thread()
    logger.info("Started cache cleanup thread")
    logger.info(f"Cache expiry set to {CACHE_EXPIRY} seconds")
    
    app.run(debug=True)
