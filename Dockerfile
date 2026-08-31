# Dockerfile for dhslab-archive (dhslab_archive.py)
#
# Runtime dependencies needed by the script:
#   - Python 3 with: boto3, pandas, tqdm, sqlalchemy
#     (botocore comes in as a dependency of boto3)
#   - The `globus` CLI on PATH, for `archive -R` (RIS archive / Globus transfer mode)
#     (installed via the `globus-cli` pip package)
#   - `tar`/`gzip` for building tarballs - provided by Ubuntu's base image
#
# NOT baked into the image (must be supplied at `docker run` time - see notes at the
# bottom of this file):
#   - AWS credentials (~/.aws/credentials), for `archive -G` (S3/Glacier mode)
#   - Globus login state (~/.globus.cfg / token cache), for `archive -R`
#   - The script's own config file (~/.dhslab-archive-config, from `init`)
#   - The data directories being archived/restored

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Base OS packages: Python 3 + pip, plus tar/gzip (present by default on Ubuntu, listed
# here for clarity) and ca-certificates (needed for pip/AWS/Globus HTTPS calls).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        tar \
        gzip \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies. globus-cli provides the `globus` command the script shells out to.
RUN pip3 install --upgrade pip \
    && pip3 install \
        boto3 \
        pandas \
        tqdm \
        sqlalchemy \
        globus-cli

# Copy the script into /usr/local/bin (as root, before switching users) rather than
# into the archiver user's home directory. This matters because the usage examples
# below bind-mount a host directory over /home/archiver to persist the config file -
# a bind mount replaces the whole target directory, so anything placed there by the
# image (like the script itself) would be hidden as soon as that mount is applied.
# /usr/local/bin is outside the home directory and already on PATH, so it's unaffected
# by that mount and works the same regardless of which UID/GID runs the container.
COPY dhslab_archive.py /usr/local/bin/dhslab-archive
RUN chmod 755 /usr/local/bin/dhslab-archive

# Run as a non-root user by default. UID/GID are left at their defaults so bind-mounted
# host directories keep sane ownership; override with --user at `docker run` time if
# you need to match a specific host UID (e.g. --user $(id -u):$(id -g)).
RUN useradd --create-home --shell /bin/bash archiver
USER archiver
WORKDIR /home/archiver

ENTRYPOINT ["python3", "/usr/local/bin/dhslab-archive"]
CMD ["--help"]

# ---------------------------------------------------------------------------
# Usage notes
#
# Build:
#   docker build -t dhslab-archive .
#
# One-time init (writes ~/.dhslab-archive-config inside the container's home dir -
# mount a host directory over it so the config persists between runs):
#   docker run --rm -it \
#     -v ~/.dhslab-archive-config-dir:/home/archiver \
#     dhslab-archive init
#
# Archive a directory to S3 Glacier (needs AWS credentials mounted read-only):
#   docker run --rm -it \
#     -v ~/.dhslab-archive-config-dir:/home/archiver \
#     -v ~/.aws:/home/archiver/.aws:ro \
#     -v /path/to/data:/data \
#     dhslab-archive archive -G /data/some_dataset
#
# Archive to the RIS archive via Globus (needs the `globus` CLI logged in on the host,
# with its token cache mounted in; the target/destination filesystem paths referenced
# by the Globus endpoint are on RIS's side, not inside this container, so no extra
# volume is needed for the destination itself):
#   docker run --rm -it \
#     -v ~/.dhslab-archive-config-dir:/home/archiver \
#     -v ~/.globus.cfg:/home/archiver/.globus.cfg:ro \
#     -v /path/to/data:/data \
#     dhslab-archive archive -R /data/some_dataset
#
# The exact Globus config/token cache location can vary by globus-cli version - check
# `globus session show` / the globus-cli docs on the host to confirm what to mount.
# ---------------------------------------------------------------------------