# Use a pre-built Python image with CUDA support
#FROM nvidia/cuda:12.1.1-base-ubuntu22.04
#FROM nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04
#FROM nvidia/cuda:12.3.2-cudnn9-runtime-ubuntu22.04
FROM nvidia/cuda:12.8.1-cudnn-runtime-ubuntu24.04
# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory in the container
WORKDIR /app

# Copy project files into the container at /app
COPY . /app

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-pip \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-venv \
    gnupg2

# Set the environment variable for the virtual environment path.
# This must be a separate command from the RUN block.
ENV VIRTUAL_ENV=/opt/venv

# Create the virtual environment
RUN python3 -m venv $VIRTUAL_ENV

# Activate the virtual environment
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Now install Python packages using the virtual environment's pip
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Expose port that the app runs on
EXPOSE 555

# Command to run the app
CMD ["uvicorn", "leela_main:app", "--host", "0.0.0.0", "--port", "555"]


