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

app = Flask(__name__)

# Configuration - Use absolute paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(CURRENT_DIR, "uploads/")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "converted/")
MAX_FILE_SIZE = 1000 * 1024 * 1024  # Increased to 1000MB max file size
CACHE_EXPIRY = 3600  # Files expire after 1 hour (in seconds)
CONVERSION_TIMEOUT = 900  # 15 minutes timeout for conversion

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create directories if they don't exist
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# In-memory cache for recent conversions
# Structure: {file_hash: {"output_path": path, "last_accessed": timestamp}}
file_cache = {}
cache_lock = threading.Lock()

# Configure allowed origins
ALLOWED_ORIGINS = [
    'https://tokhaste.com',
    'http://localhost:3000',  # For local development
    'http://localhost:5000',  # For local testing
]

def get_cors_headers(request_origin):
    """Generate appropriate CORS headers based on the origin"""
    headers = {
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Max-Age': '3600'  # Cache preflight response for 1 hour
    }
    
    # Check if the origin is allowed
    if request_origin in ALLOWED_ORIGINS:
        headers['Access-Control-Allow-Origin'] = request_origin
    elif request_origin and '*' in ALLOWED_ORIGINS:
        headers['Access-Control-Allow-Origin'] = request_origin
    elif '*' in ALLOWED_ORIGINS:
        headers['Access-Control-Allow-Origin'] = '*'
    
    return headers

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

def generate_file_hash(file_obj):
    """Generate SHA-256 hash of file contents for caching"""
    file_hash = hashlib.sha256()
    chunk_size = 8192  # Read in 8kb chunks
    file_obj.seek(0)
    
    for chunk in iter(lambda: file_obj.read(chunk_size), b''):
        file_hash.update(chunk)
    
    file_obj.seek(0)  # Reset file pointer
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
            check=True,
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
            timeout=CONVERSION_TIMEOUT  # Add timeout to prevent hanging
        )
        
        # Check if conversion was successful
        if process.returncode != 0:
            raise Exception(f"FFmpeg conversion failed: {process.stderr}")
        
        # Check if output file exists
        if not os.path.exists(output_path):
            raise Exception("Output file was not created")
            
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"Conversion timeout for {input_path}")
        raise Exception("Conversion timeout - file may be too large or complex")
    except Exception as e:
        logger.error(f"Conversion error: {str(e)}")
        raise

@app.route('/convert', methods=['POST', 'OPTIONS'])
def convert_video():
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        headers = get_cors_headers(request.origin)
        return '', 204, headers
    
    # Get CORS headers for this request
    headers = get_cors_headers(request.origin)
    
    # Check if file is uploaded
    if 'video' not in request.files:
        response = {'success': False, 'error': "No file uploaded"}
        return jsonify(response), 400, headers
    
    video_file = request.files['video']
    
    # Check if filename is empty
    if video_file.filename == '':
        response = {'success': False, 'error': "No file selected"}
        return jsonify(response), 400, headers
    
    # Save to temp file to avoid memory issues with large files
    with tempfile.NamedTemporaryFile(delete=False, dir=UPLOAD_DIR) as temp_file:
        temp_path = temp_file.name
        # Save in chunks to handle large files
        chunk_size = 4 * 1024 * 1024  # 4MB chunks
        bytes_read = 0
        
        while True:
            chunk = video_file.read(chunk_size)
            if not chunk:
                break
            temp_file.write(chunk)
            bytes_read += len(chunk)
            
            # Check file size limit
            if bytes_read > MAX_FILE_SIZE:
                temp_file.close()
                os.unlink(temp_path)
                response = {'success': False, 'error': f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)}MB"}
                return jsonify(response), 400, headers
    
    try:
        # Generate file hash for caching
        file_hash = hashlib.sha256()
        with open(temp_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                file_hash.update(chunk)
        file_hash = file_hash.hexdigest()
        
        # Check if we already have this file converted
        with cache_lock:
            if file_hash in file_cache and os.path.exists(file_cache[file_hash]["output_path"]):
                output_path = file_cache[file_hash]["output_path"]
                file_cache[file_hash]["last_accessed"] = time.time()
                output_filename = os.path.basename(output_path)
                
                # Remove temp file
                os.unlink(temp_path)
                
                logger.info(f"Using cached file: {output_path}")
                
                return jsonify({
                    'success': True,
                    'filename': output_filename,
                    'cached': True
                }), 200, headers
        
        # Generate unique filenames
        unique_video_name = generate_unique_filename(video_file.filename)
        video_path = temp_path  # Already saved to temp file
        
        # Generate output filename (replace extension with mp3)
        output_filename = os.path.splitext(unique_video_name)[0] + '.mp3'
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        # Convert the video to MP3 using FFmpeg with optimized settings
        convert_video_file(video_path, output_path)
        
        # Add to cache using file hash
        with cache_lock:
            file_cache[file_hash] = {
                "output_path": output_path,
                "last_accessed": time.time()
            }
        
        logger.info(f"File converted and cached: {output_path}")
        
        # Clean up the original temp file
        if os.path.exists(video_path):
            os.unlink(video_path)
        
        # Return success response
        response = {
            'success': True,
            'filename': output_filename,
            'cached': False
        }
        
        return jsonify(response), 200, headers
        
    except Exception as e:
        # Delete the uploaded temp file if there was an error
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        
        logger.error(f"Conversion error: {str(e)}")
        response = {'success': False, 'error': str(e)}
        return jsonify(response), 500, headers

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    """Serve the converted files directly"""
    # Get CORS headers for this request
    headers = get_cors_headers(request.origin)
    
    file_path = os.path.join(OUTPUT_DIR, secure_filename(filename))
    
    # Update last accessed time in cache
    with cache_lock:
        for file_hash, data in file_cache.items():
            if os.path.basename(data["output_path"]) == filename:
                file_cache[file_hash]["last_accessed"] = time.time()
                break
    
    if os.path.exists(file_path):
        try:
            response = send_file(
                file_path, 
                as_attachment=True,
                download_name=filename,
                mimetype='audio/mpeg'
            )
            # Add CORS headers to download response
            for key, value in headers.items():
                response.headers[key] = value
            return response
        except Exception as e:
            logger.error(f"Error sending file {file_path}: {str(e)}")
            return jsonify({"success": False, "error": f"Error sending file: {str(e)}"}), 500, headers
    else:
        return jsonify({"success": False, "error": f"File not found: {filename}"}), 404, headers

# Add CORS headers to all responses
@app.after_request
def add_cors_headers(response):
    # Only add headers if they're not already set
    headers = get_cors_headers(request.origin)
    for key, value in headers.items():
        if key not in response.headers:
            response.headers[key] = value
    return response

@app.route('/files', methods=['GET'])
def list_files():
    """List all available MP3 files (only shows cached files)"""
    # Get CORS headers for this request
    headers = get_cors_headers(request.origin)
    
    files = []
    with cache_lock:
        for data in file_cache.values():
            filename = os.path.basename(data["output_path"])
            if os.path.exists(data["output_path"]) and filename.endswith('.mp3'):
                files.append(filename)
    return jsonify({"files": files}), 200, headers

@app.route('/status', methods=['GET'])
def status():
    """Provides status information about the service"""
    # Get CORS headers for this request
    headers = get_cors_headers(request.origin)
    
    with cache_lock:
        cache_count = len(file_cache)
        cached_files = [os.path.basename(data["output_path"]) for data in file_cache.values()]
    
    return jsonify({
        "status": "running",
        "ffmpeg_version": get_ffmpeg_version(),
        "cached_files_count": cache_count,
        "cache_expiry_seconds": CACHE_EXPIRY,
        "max_file_size_mb": MAX_FILE_SIZE // (1024 * 1024)
    }), 200, headers

@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    """Admin endpoint to manually clear the cache"""
    # Get CORS headers for this request
    headers = get_cors_headers(request.origin)
    
    try:
        cleanup_expired_files()
        return jsonify({"success": True, "message": "Cache cleanup triggered"}), 200, headers
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500, headers

@app.errorhandler(413)
def request_entity_too_large(error):
    """Handle file size exceeded error from Flask/WSGI"""
    # Get CORS headers for this request
    headers = get_cors_headers(request.origin)
    return jsonify({
        "success": False,
        "error": f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)}MB"
    }), 413, headers

@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors"""
    # Get CORS headers for this request
    headers = get_cors_headers(request.origin)
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({
        "success": False,
        "error": "Server error during processing. Please try with a smaller file or contact support."
    }), 500, headers

if __name__ == '__main__':
    # Start the cleanup thread when the app starts
    start_cleanup_thread()
    logger.info("Started cache cleanup thread")
    logger.info(f"Cache expiry set to {CACHE_EXPIRY} seconds")
    logger.info(f"Max file size set to {MAX_FILE_SIZE // (1024 * 1024)}MB")
    
    # Run with optimized settings
    app.run(host='0.0.0.0', threaded=True)
