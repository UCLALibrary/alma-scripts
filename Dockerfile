FROM python:3.11-slim-bookworm

# Update base image
RUN apt-get update

# Install git, required to install the alma_api_client package from github.
RUN apt-get update && \
    apt-get install -y git

# Set correct timezone
RUN ln -sf /usr/share/zoneinfo/America/Los_Angeles /etc/localtime

# Create generic user
RUN useradd -c "generic app user" -d /home/app_user -s /bin/bash -m app_user

# Switch to application directory, creating it if needed
WORKDIR /home/app_user/project

# Make sure app_user owns app directory, if WORKDIR created it:
# https://github.com/docker/docs/issues/13574
RUN chown -R app_user:app_user /home/app_user

# Change context to app_user for remaining steps
USER app_user

# Copy application files to image, and ensure app_user owns everything
COPY --chown=app_user:app_user . .

# Include local python bin into app_user's path, mostly for pip
ENV PATH=/home/app_user/.local/bin:${PATH}

# Make sure pip is up to date, and don't complain if it isn't yet
RUN pip install --upgrade pip --disable-pip-version-check

# Install requirements for this application
RUN pip install --no-cache-dir -r requirements.txt --user --no-warn-script-location
