FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . /app

# Set Python path
ENV PYTHONPATH=/app

CMD ["bash"]