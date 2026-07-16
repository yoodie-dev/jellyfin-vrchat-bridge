# Use a lightweight Python base image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Install the required Python packages
RUN pip install --no-cache-dir fastapi uvicorn websockets httpx python-dotenv

# Copy files into the container
COPY . .

# Tell Docker what port the app uses
EXPOSE 8000

# Command to run the application
CMD ["python", "bridge.py"]