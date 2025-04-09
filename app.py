from flask import Flask, request, jsonify, send_file
import os
import uuid
import re
import subprocess
import time
import threading
import logging
from functools import lru_cache
import hashlib
import tempfile
from werkzeug.utils import secure_filename
from flask_cors import CORS

app = Flask(__name__)
# Apply CORS globally with support for credentials
CORS(app, resources={
    r"/*": {
        "origins": ["https://tokhaste.com", "http://localhost:3000", "http://localhost:5000"],
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Configuration - Use absolute paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(CURRENT_DIR, "uploads/")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "converted/")
TEMP_DIR = os.path.join(CURRENT_DIR, "temp/")
MAX_FILE_SIZE = 1000 * 1024 * 1024  # 1000MB max file size
CACHE_EXPIRY = 3600  # Files expire after 1 hour (in seconds)
CONVERSION_TIMEOUT = 900  # 15 minutes timeout for conversion

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Make sure all required directories exist
for directory in [UPLOAD_DIR, OUTPUT_DIR, TEMP_DIR]:
    os.makedirs(directory, exist_ok=True)

# In-memory cache for recent conversions
# Structure: {file_hash: {"output_path": path, "last_accessed": timestamp}}
file_cache = {}
cache_lock = threading.Lock()
ongoing_conversions = {}  # Track {temp_file_path: status}
conversion_lock = threading.Lock()

def sanitize_filename(name):
    """Remove any path info and sanitize the file name"""
    name = secure_filename(name)  # Use Werkzeug's secure_filename
    name = name.replace(' ', '_')
    return name

def generate_unique_filename(original_name):
    """Generate a unique filename based on the original name"""
    filename, extension = os.path.splitext(original_name)
    unique_id = uuid.uuid4().hex[:10]
    return f"{sanitize_filename(filename)}_{unique_id}{extension}"

def generate_file_hash(file_path):
    """Generate SHA-256 hash of file contents for caching"""
    file_hash = hashlib.sha256()
    chunk_size = 8192  # Read in 8kb chunks
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            file_hash.update(chunk)
    
    return file_hash.hexdigest()

@lru_cache(maxsize=10)
def get_ffmpeg_version():
    """Cache the FFmpeg version to avoid repeated subprocess calls"""
    try:
        process = subprocess.run(
            ["ffmpeg", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=10  # Add timeout to prevent hanging
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
                    if os.path.exists(data["output_path"]):
                        os.remove(data["output_path"])
                        logger.info(f"Removed expired file: {data['output_path']}")
                    expired_keys.append(key)
                except Exception as e:
                    logger.error(f"Error removing file {data['output_path']}: {str(e)}")
        
        # Remove expired entries from cache
        for key in expired_keys:
            del file_cache[key]
    
    # Also clean up any abandoned files in TEMP_DIR
    try:
        for filename in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, filename)
            if os.path.isfile(file_path) and (time.time() - os.path.getmtime(file_path)) > CACHE_EXPIRY:
                os.remove(file_path)
                logger.info(f"Removed abandoned temp file: {file_path}")
    except Exception as e:
        logger.error(f"Error cleaning temp directory: {str(e)}")

def start_cleanup_thread():
    """Start a background thread to periodically clean up expired files"""
    def cleanup_task():
        while True:
            cleanup_expired_files()
            time.sleep(300)  # Run every 5 minutes

    cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
    cleanup_thread.start()

def convert_video_file(input_path, output_path):
    """Convert video to MP3 using FFmpeg with optimized settings for large files"""
    try:
        # For larger files, use more conservative settings
        ffmpeg_command = [
            "ffmpeg", 
            "-y",  # Overwrite output files without asking
            "-i", input_path, 
            "-vn",  # No video
            "-ar", "44100",  # Audio sample rate
            "-ac", "2",  # Stereo
            "-b:a", "192k",  # Bitrate
            "-threads", "2",  # Limit threads to avoid memory issues
            "-preset", "ultrafast",  # Fastest preset to reduce processing time
            "-f", "mp3",  # Force mp3 format
            output_path
        ]
        
        # Execute with timeout
        process = subprocess.run(
            ffmpeg_command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=CONVERSION_TIMEOUT  # Add timeout to prevent hanging
        )
        
        # Check if conversion was successful
        if process.returncode != 0:
            logger.error(f"FFmpeg error: {process.stderr}")
            raise Exception(f"FFmpeg conversion failed with code {process.returncode}")
        
        # Check if output file exists and has content
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            raise Exception("Output file was not created or is empty")
            
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"Conversion timeout for {input_path}")
        raise Exception("Conversion timeout - file may be too large or complex")
    except Exception as e:
        logger.error(f"Conversion error: {str(e)}")
        raise

@app.route('/convert', methods=['POST', 'OPTIONS'])
def convert_video():
    """Handle video conversion requests"""
    # Check if file is uploaded
    if 'video' not in request.files:
        response = {'success': False, 'error': "No file uploaded"}
        return jsonify(response), 400
    
    video_file = request.files['video']
    
    # Check if filename is empty
    if video_file.filename == '':
        response = {'success': False, 'error': "No file selected"}
        return jsonify(response), 400
    
    try:
        # Create a temporary file for the upload
        os.makedirs(TEMP_DIR, exist_ok=True)
        temp_file_path = os.path.join(TEMP_DIR, f"upload_{uuid.uuid4().hex}.tmp")
        
        # Save upload in chunks to prevent memory issues
        chunk_size = 4 * 1024 * 1024  # 4MB chunks
        bytes_read = 0
        
        with open(temp_file_path, 'wb') as temp_file:
            while True:
                chunk = video_file.read(chunk_size)
                if not chunk:
                    break
                temp_file.write(chunk)
                bytes_read += len(chunk)
                
                # Check file size limit
                if bytes_read > MAX_FILE_SIZE:
                    os.unlink(temp_file_path)
                    response = {'success': False, 'error': f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)}MB"}
                    return jsonify(response), 400
        
        # Calculate hash from the saved file
        file_hash = generate_file_hash(temp_file_path)
        
        # Check if we already have this file converted
        with cache_lock:
            if file_hash in file_cache and os.path.exists(file_cache[file_hash]["output_path"]):
                output_path = file_cache[file_hash]["output_path"]
                file_cache[file_hash]["last_accessed"] = time.time()
                output_filename = os.path.basename(output_path)
                
                # Remove temp file
                os.unlink(temp_file_path)
                
                logger.info(f"Using cached file: {output_path}")
                
                return jsonify({
                    'success': True,
                    'filename': output_filename,
                    'cached': True
                })
        
        # Generate unique output filename
        unique_video_name = generate_unique_filename(video_file.filename)
        output_filename = os.path.splitext(unique_video_name)[0] + '.mp3'
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        # Convert the video to MP3
        convert_video_file(temp_file_path, output_path)
        
        # Check if conversion was successful
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            raise Exception("Conversion failed to produce valid output file")
        
        # Add to cache
        with cache_lock:
            file_cache[file_hash] = {
                "output_path": output_path,
                "last_accessed": time.time()
            }
        
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        
        logger.info(f"File converted successfully: {output_path}")
        
        # Return success response
        return jsonify({
            'success': True,
            'filename': output_filename,
            'cached': False
        })
        
    except Exception as e:
        logger.error(f"Error in convert_video: {str(e)}")
        
        # Clean up temp file if it exists
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except:
                pass
        
        return jsonify({
            'success': False, 
            'error': str(e)
        }), 500

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    """Serve the converted files directly"""
    safe_filename = secure_filename(filename)
    file_path = os.path.join(OUTPUT_DIR, safe_filename)
    
    # Update last accessed time in cache
    with cache_lock:
        for file_hash, data in file_cache.items():
            if os.path.basename(data["output_path"]) == safe_filename:
                file_cache[file_hash]["last_accessed"] = time.time()
                break
    
    if os.path.exists(file_path):
        try:
            return send_file(
                file_path, 
                as_attachment=True,
                download_name=safe_filename,
                mimetype='audio/mpeg'
            )
        except Exception as e:
            logger.error(f"Error sending file {file_path}: {str(e)}")
            return jsonify({"success": False, "error": f"Error sending file: {str(e)}"}), 500
    else:
        return jsonify({"success": False, "error": f"File not found: {filename}"}), 404

@app.route('/files', methods=['GET'])
def list_files():
    """List all available MP3 files (only shows cached files)"""
    files = []
    with cache_lock:
        for data in file_cache.values():
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
        "cache_expiry_seconds": CACHE_EXPIRY,
        "max_file_size_mb": MAX_FILE_SIZE // (1024 * 1024),
        "headers": dict(request.headers)  # Debug: show received headers
    })

@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    """Admin endpoint to manually clear the cache"""
    try:
        cleanup_expired_files()
        return jsonify({"success": True, "message": "Cache cleanup triggered"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.errorhandler(413)
def request_entity_too_large(error):
    """Handle file size exceeded error from Flask/WSGI"""
    return jsonify({
        "success": False,
        "error": f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)}MB"
    }), 413

@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors"""
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({
        "success": False,
        "error": "Server error during processing. Please try with a smaller file or contact support."
    }), 500

@app.route('/', methods=['GET'])
def index():
    """Simple index page to verify the server is running"""
    return jsonify({
        "status": "Video to MP3 conversion service is running",
        "version": "2.0",
        "endpoints": ["/convert", "/download/<filename>", "/files", "/status", "/clear-cache"]
    })

if __name__ == '__main__':
    # Start the cleanup thread when the app starts
    start_cleanup_thread()
    logger.info("Started cache cleanup thread")
    logger.info(f"Cache expiry set to {CACHE_EXPIRY} seconds")
    logger.info(f"Max file size set to {MAX_FILE_SIZE // (1024 * 1024)}MB")
    
    # Run with optimized settings
    app.run(host='0.0.0.0', port=5000, threaded=True)
