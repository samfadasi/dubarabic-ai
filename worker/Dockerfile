FROM node:22-bookworm-slim

# ffmpeg + fonts (Arabic-ready) + fontconfig for subtitle renderer
RUN apt-get update && apt-get install -y --no-install-recommends \
  ffmpeg \
  fontconfig \
  fonts-noto \
  fonts-noto-core \
  fonts-noto-extra \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first (better layer caching)
COPY package.json package-lock.json* ./
RUN npm ci || npm install

# Copy source and build
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# Run compiled output
CMD ["node", "dist/index.js"]