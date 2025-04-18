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
import requests

app = Flask(__name__)

# Configuration - Use absolute paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(CURRENT_DIR, "uploads/")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "converted/")
TEMP_DIR = os.path.join(CURRENT_DIR, "temp/")  # For temporary compressed videos
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB max file size
CACHE_EXPIRY = 3600  # Files expire after 1 hour (in seconds)

# Cloudflare Turnstile Configuration
# Site key from JS: 0x4AAAAAABHoxdccCZ_9Cezk
# You need to use the secret key paired with this site key
TURNSTILE_SECRET_KEY = "0x4AAAAAABHoxYr9SKSH_1ZBB4LpXbr_0sQ"  # This is a placeholder - replace with your actual secret key
TURNSTILE_VERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create directories if they don't exist
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# In-memory cache for recent conversions
# Structure: {file_hash: {"output_path": path, "last_accessed": timestamp}}
file_cache = {}
cache_lock = threading.Lock()

def verify_turnstile_token(token, remote_ip=None):
    """Verify a Cloudflare Turnstile token with the Cloudflare API"""
    try:
        data = {
            'secret': TURNSTILE_SECRET_KEY,
            'response': token
        }
        
        # Add the user's IP if provided (optional but recommended by Cloudflare)
        if remote_ip:
            data['remoteip'] = remote_ip
            
        logger.info(f"Verifying Turnstile token for IP: {remote_ip if remote_ip else 'Not provided'}")
        
        response = requests.post(TURNSTILE_VERIFY_URL, data=data)
        result = response.json()
        
        # Log verification attempt
        if result.get('success'):
            logger.info("Turnstile verification successful")
        else:
            error_codes = result.get('error-codes', [])
            logger.warning(f"Turnstile verification failed: {error_codes}")
            
        return result.get('success', False)
    except Exception as e:
        logger.error(f"Error verifying Turnstile token: {str(e)}")
        return False

def sanitize_filename(name):
    """Remove any path info and sanitize the file name"""
    name = os.path.basename(name)
    name = name.replace(' ', '_')
    name = re.sub(r'[^A-Za-z0-9_\-\.]', '', name)
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

def get_video_duration(video_path):
    """Get the duration of a video file in seconds using FFprobe"""
    try:
        cmd = [
            "ffprobe", 
            "-v", "error", 
            "-show_entries", "format=duration", 
            "-of", "default=noprint_wrappers=1:nokey=1", 
            video_path
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        duration = float(result.stdout.strip())
        return duration
    except Exception as e:
        logger.error(f"Error getting video duration: {str(e)}")
        return 0  # Return 0 if duration cannot be determined

@lru_cache(maxsize=10)
def get_ffmpeg_version():
    """Cache the FFmpeg version to avoid repeated subprocess calls"""
    try:
        process = subprocess.run(
            ["ffmpeg", "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
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
            
    # Also clean up any leftover files in the temp directory
    for filename in os.listdir(TEMP_DIR):
        file_path = os.path.join(TEMP_DIR, filename)
        try:
            if os.path.isfile(file_path):
                file_age = current_time - os.path.getmtime(file_path)
                if file_age > CACHE_EXPIRY:
                    os.remove(file_path)
                    logger.info(f"Removed expired temp file: {file_path}")
        except Exception as e:
            logger.error(f"Error cleaning temp file {file_path}: {str(e)}")

def start_cleanup_thread():
    """Start a background thread to periodically clean up expired files"""
    def cleanup_task():
        while True:
            cleanup_expired_files()
            time.sleep(300)  # Run every 5 minutes

    cleanup_thread = threading.Thread(target=cleanup_task, daemon=True)
    cleanup_thread.start()

# Global CORS headers
CORS_HEADERS = {
    'Access-Control-Allow-Origin': 'https://tokhaste.com',  # Change to specific domain in production
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '3600'  # Cache preflight response for 1 hour
}

@app.route('/convert', methods=['POST', 'OPTIONS'])
def convert_video():
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 204, CORS_HEADERS  # Return 204 No Content for OPTIONS
    
    # Verify Cloudflare Turnstile token
    token = request.form.get('cf-turnstile-response')
    remote_ip = request.remote_addr
    
    if not token:
        logger.warning("Turnstile token missing in request")
        response = {'success': False, 'error': "Security verification required"}
        return jsonify(response), 400
    
    # Verify the token with Cloudflare
    if not verify_turnstile_token(token, remote_ip):
        logger.warning(f"Invalid Turnstile token from IP: {remote_ip}")
        response = {'success': False, 'error': "Security verification failed"}
        return jsonify(response), 403
    
    # Check if file is uploaded
    if 'video' not in request.files:
        response = {'success': False, 'error': "No file uploaded"}
        return jsonify(response), 400
    
    video_file = request.files['video']
    
    # Check if filename is empty
    if video_file.filename == '':
        response = {'success': False, 'error': "No file selected"}
        return jsonify(response), 400
    
    # Validate file size (Flask doesn't have built-in size checking before reading)
    video_file.seek(0, os.SEEK_END)
    file_size = video_file.tell()
    video_file.seek(0)  # Reset file pointer
    
    if file_size > MAX_FILE_SIZE:
        response = {'success': False, 'error': f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)}MB"}
        return jsonify(response), 400
    
    try:
        # Generate file hash for caching
        file_hash = generate_file_hash(video_file)
        
        # Check if we already have this file converted
        with cache_lock:
            if file_hash in file_cache and os.path.exists(file_cache[file_hash]["output_path"]):
                output_path = file_cache[file_hash]["output_path"]
                file_cache[file_hash]["last_accessed"] = time.time()
                output_filename = os.path.basename(output_path)
                
                logger.info(f"Using cached file: {output_path}")
                
                return jsonify({
                    'success': True,
                    'filename': output_filename,
                    'cached': True
                })
        
        # Generate unique filenames
        unique_video_name = generate_unique_filename(video_file.filename)
        video_path = os.path.join(UPLOAD_DIR, unique_video_name)
        
        # Generate output filename (replace extension with mp3)
        output_filename = os.path.splitext(unique_video_name)[0] + '.mp3'
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        # Save the uploaded file
        video_file.save(video_path)
        
        # Check video duration (keeping for informational purposes)
        duration = get_video_duration(video_path)
        logger.info(f"Video duration: {duration} seconds")
        
        # Source video to use for conversion - always use the original video
        source_video_path = video_path
        
        # Get format and bitrate (if provided)
        format_type = request.form.get('format', 'mp3')
        bitrate = request.form.get('bitrate', '192k')
        
        # Validate format and bitrate
        if format_type not in ['mp3', 'aac', 'wav', 'ogg']:
            format_type = 'mp3'  # Default to mp3 if invalid
        
        if bitrate not in ['128k', '192k', '256k', '320k']:
            bitrate = '192k'  # Default to 192k if invalid
        
        # Adjust output filename based on format
        if format_type != 'mp3':
            output_filename = os.path.splitext(unique_video_name)[0] + f'.{format_type}'
            output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        # FFmpeg command to extract audio
        ffmpeg_command = [
            "ffmpeg", 
            "-i", source_video_path, 
            "-vn",  # No video
            "-ar", "44100",  # Audio sample rate
            "-ac", "2",  # Stereo
            "-b:a", bitrate,  # Use selected bitrate
            "-threads", "0",  # Use all available threads
            "-bufsize", "3M",  # Smaller buffer size
            "-maxrate", "384k",  # Maximum bitrate
            "-preset", "ultrafast",  # Use fastest preset
            "-f", format_type,  # Use selected format
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
        
        # Add to cache using file hash
        with cache_lock:
            file_cache[file_hash] = {
                "output_path": output_path,
                "last_accessed": time.time()
            }
        
        logger.info(f"File converted and cached: {output_path}")
        
        # Clean up temporary files
        os.remove(video_path)
        
        # Return success response
        response = {
            'success': True,
            'filename': output_filename,
            'duration': duration,
            'format': format_type,
            'bitrate': bitrate,
            'cached': False
        }
        
        return jsonify(response)
        
    except Exception as e:
        # Log the full error for debugging
        logger.error(f"Conversion error: {str(e)}")
        
        # Delete the uploaded file if there was an error
        if 'video_path' in locals() and os.path.exists(video_path):
            os.remove(video_path)
        
        response = {'success': False, 'error': str(e)}
        return jsonify(response), 500

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    """Serve the converted files directly"""
    file_path = os.path.join(OUTPUT_DIR, filename)
    
    # Update last accessed time in cache
    with cache_lock:
        for file_hash, data in file_cache.items():
            if os.path.basename(data["output_path"]) == filename:
                file_cache[file_hash]["last_accessed"] = time.time()
                break
    
    if os.path.exists(file_path):
        response = send_file(
            file_path, 
            as_attachment=True,
            download_name=filename,
            mimetype='audio/mpeg'
        )
        # Add CORS headers to download response
        for key, value in CORS_HEADERS.items():
            response.headers[key] = value
        return response
    else:
        return f"File not found: {filename}", 404

# Add CORS headers to all responses
@app.after_request
def add_cors_headers(response):
    # Only add headers if they're not already there
    for key, value in CORS_HEADERS.items():
        if key not in response.headers:
            response.headers[key] = value
    return response

@app.route('/files', methods=['GET'])
def list_files():
    """List all available MP3 files (only shows cached files)"""
    files = []
    with cache_lock:
        for data in file_cache.values():
            filename = os.path.basename(data["output_path"])
            if os.path.exists(data["output_path"]):
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
        "turnstile_enabled": True,
        "turnstile_site_key": "0x4AAAAAABHoxdccCZ_9Cezk"  # Add site key for frontend reference
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
    logger.info("Cloudflare Turnstile protection enabled")
    
    # Run with optimized settings
    app.run(host='0.0.0.0', threaded=True)
