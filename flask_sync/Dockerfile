# Use the official Python image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Flask application code
COPY app.py app.py

# Expose the Flask port
EXPOSE 5000

# Command to run the Flask application
CMD ["python", "app.py"]
