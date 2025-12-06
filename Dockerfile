FROM node:20-alpine

WORKDIR /app

# Install deps
COPY package*.json ./
RUN npm ci --only=production

# Copy the rest of your server code
COPY . .

ENV PORT=4000
EXPOSE 4000

CMD ["npm", "start"]
