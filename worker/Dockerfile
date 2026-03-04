FROM node:22-bookworm-slim

# ffmpeg + ffprobe + Arabic fonts for subtitle burn-in
RUN apt-get update && apt-get install -y --no-install-recommends \
  ffmpeg \
  fontconfig \
  fonts-noto \
  fonts-noto-core \
  fonts-noto-extra \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# deps first for cache
COPY package.json package-lock.json* ./
RUN npm ci || npm install

# build TS
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# run compiled output
CMD ["node", "dist/index.js"]