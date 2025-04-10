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

app = Flask(__name__)

# Configuration - Use absolute paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(CURRENT_DIR, "uploads/")
OUTPUT_DIR = os.path.join(CURRENT_DIR, "converted/")
TEMP_DIR = os.path.join(CURRENT_DIR, "temp/")  # For temporary compressed videos
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB max file size
CACHE_EXPIRY = 3600  # Files expire after 1 hour (in seconds)
VIDEO_DURATION_THRESHOLD = 240  # 4 minutes in seconds

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

def compress_video(input_path, output_path):
    """Compress video to reduce size while maintaining reasonable quality"""
    try:
        # Use more efficient compression settings for large videos
        cmd = [
            "ffmpeg",
            "-i", input_path,
            "-c:v", "libx264",
            "-crf", "30",  # Higher CRF means more compression (lower quality)
            "-preset", "ultrafast",  # Fastest encoding
            "-c:a", "aac",
            "-b:a", "128k",
            "-vf", "scale=-2:720",  # Resize to 720p
            "-threads", "0",
            "-y",  # Overwrite output file if it exists
            output_path
        ]
        
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            raise Exception(f"Video compression failed: {result.stderr}")
        
        return True
    except Exception as e:
        logger.error(f"Compression error: {str(e)}")
        return False

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
        
        # Check video duration
        duration = get_video_duration(video_path)
        logger.info(f"Video duration: {duration} seconds")
        
        # Source video to use for conversion
        source_video_path = video_path
        compressed = False
        
        # Compress the video if it's longer than the threshold
        if duration > VIDEO_DURATION_THRESHOLD:
            compressed_path = os.path.join(TEMP_DIR, f"compressed_{unique_video_name}")
            logger.info(f"Compressing video (duration: {duration}s) before conversion")
            
            if compress_video(video_path, compressed_path):
                source_video_path = compressed_path
                compressed = True
                logger.info(f"Video successfully compressed: {compressed_path}")
            else:
                logger.warning("Compression failed, using original video")
        
        # Modify FFmpeg command to be more memory-efficient
        ffmpeg_command = [
            "ffmpeg", 
            "-i", source_video_path, 
            "-vn",  # No video
            "-ar", "44100",  # Audio sample rate
            "-ac", "2",  # Stereo
            "-b:a", "192k",  # Bitrate
            "-threads", "0",  # Use all available threads
            "-bufsize", "3M",  # Smaller buffer size
            "-maxrate", "384k",  # Maximum bitrate
            "-preset", "ultrafast",  # Use fastest preset
            "-f", "mp3",  # Force mp3 format
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
        if compressed and os.path.exists(source_video_path) and source_video_path != video_path:
            os.remove(source_video_path)
        
        # Return success response
        response = {
            'success': True,
            'filename': output_filename,
            'compressed': compressed,
            'duration': duration,
            'cached': False
        }
        
        return jsonify(response)
        
    except Exception as e:
        # Log the full error for debugging
        logger.error(f"Conversion error: {str(e)}")
        
        # Delete the uploaded file if there was an error
        if 'video_path' in locals() and os.path.exists(video_path):
            os.remove(video_path)
        
        # Clean up compressed file if it exists
        if 'compressed' in locals() and compressed and 'source_video_path' in locals() and os.path.exists(source_video_path):
            os.remove(source_video_path)
        
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
        "video_duration_threshold": VIDEO_DURATION_THRESHOLD,
        "max_file_size_mb": MAX_FILE_SIZE // (1024 * 1024)
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
    logger.info(f"Videos longer than {VIDEO_DURATION_THRESHOLD} seconds will be compressed")
    
    # Run with optimized settings
    app.run(host='0.0.0.0', threaded=True)
