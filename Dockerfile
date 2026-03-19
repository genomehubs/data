# Use Python 3.12 base image (matches your conda environment)
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    tar \
    gzip \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app

# Install Python dependencies with tol-sdk compatible versions
# Using requirements-docker.txt which pins compatible versions
RUN pip install --no-cache-dir -r requirements-docker.txt

# Verify installations
RUN python -c "import genomehubs; print('genomehubs installed')" && \
    python -c "import tol; print('tol-sdk installed')" && \
    python -c "import click; print(f'Click {click.__version__}')" && \
    python -c "import pydantic; print(f'Pydantic {pydantic.__version__}')" && \
    echo "All dependencies verified and compatible!"

# Set the entrypoint for running flows
ENTRYPOINT ["python"]
CMD ["-m", "prefect.cli"]
