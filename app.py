from flask import Flask, request, jsonify, send_file, make_response
import os
import uuid
import re
import subprocess
import io
import time
import tempfile
from collections import defaultdict, deque
import mimetypes
from werkzeug.utils import secure_filename

app = Flask(__name__)

# Configuration
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB max file size
MAX_REQUESTS_PER_HOUR = 10  # Limit requests per IP
MAX_RECOMMENDED_SIZE = 100 * 1024 * 1024  # 100MB recommended size

# In-memory cache for converted files
file_cache = {}  # {file_id: {'data': binary_data, 'created_at': timestamp}}
CACHE_EXPIRY = 3600  # Cache expiry in seconds (1 hour)

# Rate limiting data structure
ip_request_tracker = defaultdict(lambda: deque(maxlen=MAX_REQUESTS_PER_HOUR))

def clean_cache():
    """Remove expired items from cache"""
    current_time = time.time()
    expired_keys = [k for k, v in file_cache.items() if current_time - v['created_at'] > CACHE_EXPIRY]
    for key in expired_keys:
        del file_cache[key]

def sanitize_filename(name):
    """Remove any path info and sanitize the file name"""
    # Get the base name
    name = os.path.basename(name)
    # Replace spaces with underscores
    name = name.replace(' ', '_')
    # Remove special characters
    name = re.sub(r'[^A-Za-z0-9_\-\.]', '', name)
    return name

def generate_unique_id():
    """Generate a unique ID for the file"""
    return uuid.uuid4().hex[:10]

def check_rate_limit(ip_address):
    """Check if IP has exceeded rate limit"""
    current_time = time.time()
    # Remove requests older than 1 hour
    while ip_request_tracker[ip_address] and ip_request_tracker[ip_address][0] < current_time - 3600:
        ip_request_tracker[ip_address].popleft()
    
    # Check if limit reached
    if len(ip_request_tracker[ip_address]) >= MAX_REQUESTS_PER_HOUR:
        return False
    
    # Add current request timestamp
    ip_request_tracker[ip_address].append(current_time)
    return True

@app.route('/convert', methods=['POST', 'OPTIONS'])
def convert_video():
    # Handle preflight OPTIONS request
    if request.method == 'OPTIONS':
        return '', 200
    
    response = {}
    
    # Get client IP for rate limiting
    client_ip = request.remote_addr
    
    # Check rate limit
    if not check_rate_limit(client_ip):
        response['success'] = False
        response['error'] = f"Rate limit exceeded. Maximum {MAX_REQUESTS_PER_HOUR} conversions per hour."
        return jsonify(response), 429
    
    # Check if file is uploaded
    if 'video' not in request.files:
        response['success'] = False
        response['error'] = "Upload failed: no file found"
        return jsonify(response), 400
    
    video_file = request.files['video']
    
    # Check if filename is empty
    if video_file.filename == '':
        response['success'] = False
        response['error'] = "Upload failed: no file selected"
        return jsonify(response), 400
    
    # Validate file size
    video_file.seek(0, os.SEEK_END)
    file_size = video_file.tell()
    video_file.seek(0)  # Reset file pointer
    
    if file_size > MAX_FILE_SIZE:
        response['success'] = False
        response['error'] = f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)}MB"
        return jsonify(response), 400
    
    if file_size > MAX_RECOMMENDED_SIZE:
        # Still process but warn the user
        response['warning'] = f"Try using a shorter video (under 100MB) for faster processing"
    
    # Get format and bitrate options
    output_format = request.form.get('format', 'mp3').lower()
    bitrate = request.form.get('bitrate', '192k').lower()
    
    # Validate format option
    valid_formats = ['mp3', 'aac', 'wav']
    if output_format not in valid_formats:
        output_format = 'mp3'  # Default to MP3
    
    # Validate bitrate option
    valid_bitrates = ['128k', '192k', '320k']
    if bitrate not in valid_bitrates:
        bitrate = '192k'  # Default to 192k
    
    try:
        # Create temporary files for processing
        with tempfile.NamedTemporaryFile(delete=False) as temp_input:
            video_file.save(temp_input.name)
            temp_input_path = temp_input.name
        
        # Define output extension based on format
        format_extensions = {
            'mp3': '.mp3',
            'aac': '.aac',
            'wav': '.wav'
        }
        
        # Generate a unique ID for the output file
        unique_id = generate_unique_id()
        sanitized_name = sanitize_filename(os.path.splitext(video_file.filename)[0])
        output_filename = f"{sanitized_name}_{unique_id}{format_extensions[output_format]}"
        
        # Create temporary output file
        with tempfile.NamedTemporaryFile(delete=False) as temp_output:
            temp_output_path = temp_output.name
        
        # Set FFmpeg parameters based on format
        format_params = {
            'mp3': ['-acodec', 'libmp3lame', '-b:a', bitrate],
            'aac': ['-acodec', 'aac', '-b:a', bitrate],
            'wav': ['-acodec', 'pcm_s16le']  # WAV doesn't use bitrate
        }
        
        # Convert the video to audio using FFmpeg
        ffmpeg_command = [
            "ffmpeg", 
            "-i", temp_input_path, 
            "-vn", 
            "-ar", "44100", 
            "-ac", "2"
        ]
        
        # Add format-specific parameters
        ffmpeg_command.extend(format_params[output_format])
        
        # Add output path
        ffmpeg_command.append(temp_output_path)
        
        process = subprocess.run(
            ffmpeg_command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Check for FFmpeg errors
        if process.returncode != 0:
            error_message = process.stderr.lower()
            
            # Check for specific error patterns and provide better messages
            if "unknown encoder" in error_message or "invalid codec" in error_message:
                raise Exception("FFmpeg error: unsupported codec")
            elif "invalid data" in error_message:
                raise Exception("FFmpeg error: invalid video file format")
            else:
                raise Exception(f"FFmpeg conversion failed: {process.stderr}")
        
        # Read the output file into memory
        with open(temp_output_path, 'rb') as f:
            audio_data = f.read()
        
        # Store in cache
        file_cache[unique_id] = {
            'data': audio_data,
            'filename': output_filename,
            'created_at': time.time(),
            'format': output_format
        }
        
        # Clean up temporary files
        os.unlink(temp_input_path)
        os.unlink(temp_output_path)
        
        # Run cache cleanup
        clean_cache()
        
        # Return success response with file ID for downloading
        response['success'] = True
        response['file_id'] = unique_id
        response['filename'] = output_filename
        response['format'] = output_format
        response['bitrate'] = bitrate if output_format != 'wav' else 'lossless'
        
        if 'warning' in response:
            # Keep the warning if it exists
            pass
        
        return jsonify(response)
        
    except Exception as e:
        # Clean up temporary files if they exist
        for path in [temp_input_path, temp_output_path]:
            if 'path' in locals() and os.path.exists(path):
                os.unlink(path)
        
        response['success'] = False
        response['error'] = str(e)
        return jsonify(response), 500

@app.route('/download/<file_id>', methods=['GET'])
def download_file(file_id):
    """Serve the converted files from memory cache"""
    # Check if file exists in cache
    if file_id not in file_cache:
        return "File not found or has expired", 404
    
    # Get file from cache
    file_data = file_cache[file_id]
    
    # Set correct MIME type based on format
    format_mimetypes = {
        'mp3': 'audio/mpeg',
        'aac': 'audio/aac',
        'wav': 'audio/wav'
    }
    
    # Prepare response with file data
    response = make_response(file_data['data'])
    response.headers['Content-Type'] = format_mimetypes.get(file_data['format'], 'application/octet-stream')
    response.headers['Content-Disposition'] = f'attachment; filename="{file_data["filename"]}"'
    
    return response

# Add CORS headers to all responses
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'  # Change to your specific domain in production
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

# Route to check API status
@app.route('/status', methods=['GET'])
def status():
    clean_cache()  # Clean expired files
    return jsonify({
        "status": "active",
        "cache_size": len(file_cache),
        "formats_supported": ["mp3", "aac", "wav"],
        "bitrates_supported": ["128k", "192k", "320k"]
    })

if __name__ == '__main__':
    app.run(debug=True)
