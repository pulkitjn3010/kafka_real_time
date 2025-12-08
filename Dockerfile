FROM apache/spark:3.5.1

# Switch to root user for installations
USER 0

# Set working directory
WORKDIR /opt/spark/work-dir

# Copy Python requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy Spark application
COPY spark_consumer.py .

# Create directories with proper permissions
RUN mkdir -p /delta-lake /tmp/multi_sink_checkpoint && \
    chmod -R 777 /delta-lake /tmp/multi_sink_checkpoint

# Set environment variables for better logging
ENV PYTHONUNBUFFERED=1

# Stay as root (for development)
# In production, you'd want: USER 185 (spark user)