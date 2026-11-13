# Use a lightweight Python base image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Install the required Python packages. websockets is floored at 14 because
# the WebSocket listener passes additional_headers= (see
# jellyfin_websocket_listener) - that kwarg was named extra_headers= before
# 14.0, so an older resolve would break the Jellyfin socket connection.
RUN pip install --no-cache-dir fastapi uvicorn "websockets>=14" httpx python-dotenv

# Copy files into the container
COPY . .

# Tell Docker what port the app uses
EXPOSE 8000

# Command to run the application
CMD ["python", "bridge.py"]