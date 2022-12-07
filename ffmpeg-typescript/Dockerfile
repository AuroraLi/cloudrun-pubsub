FROM node:12-slim
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y ffmpeg

WORKDIR /usr/src/app

# Copy distributable code
COPY package*.json ./
COPY tsconfig*.json ./

# Install ejs
COPY . .
RUN npm install
RUN npm run build

#Expose the right port
EXPOSE 8080

# Run our app
CMD ["node", "build/app.js"]